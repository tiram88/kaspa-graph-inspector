package batch

import (
	"os"

	"github.com/go-pg/pg/v10"
	databasePackage "github.com/kaspa-live/kaspa-graph-inspector/processing/database"
	"github.com/kaspa-live/kaspa-graph-inspector/processing/infrastructure/logging"
	"github.com/kaspa-live/kaspa-graph-inspector/processing/infrastructure/network/rpcclient"
	"github.com/kaspa-live/kaspa-graph-inspector/processing/infrastructure/queue"
	"github.com/kaspa-live/kaspa-graph-inspector/processing/processing/block"
	"github.com/kaspanet/kaspad/domain/consensus/model/externalapi"
	"github.com/pkg/errors"
)

const MaxSupportedMissingDependencies = 600
const PrePruningPointDaaScoreThreshold = 500

var log = logging.Logger()

type Batch struct {
	database     *databasePackage.Database
	rpcClient    *rpcclient.RPCClient
	blocks       []*BlockAndHash
	hashes       map[externalapi.DomainHash]*BlockAndHash
	pruningBlock *block.Block
}

// type Block externalapi.DomainBlock

type BlockAndHash struct {
	*block.Block
	children map[externalapi.DomainHash]*BlockAndHash
}

func (ba *BlockAndHash) Hash() *externalapi.DomainHash {
	return ba.Block.Hash
}

func New(database *databasePackage.Database, rpcClient *rpcclient.RPCClient, prunningBlock *block.Block) *Batch {
	batch := &Batch{
		database:     database,
		rpcClient:    rpcClient,
		blocks:       make([]*BlockAndHash, 0),
		hashes:       make(map[externalapi.DomainHash]*BlockAndHash),
		pruningBlock: prunningBlock,
	}
	return batch
}

// InScope returns true if `block` has a greater DAA score than that of the pruning block (minus an allowed threshold)
// or if no pruning block is defined
func (b *Batch) InScope(block *block.Block) bool {
	return b.pruningBlock == nil || b.pruningBlock.Domain.Header.DAAScore() <= block.Domain.Header.DAAScore()+PrePruningPointDaaScoreThreshold
}

// IgnoreParents returns true if `block` has a DAA score lower than that of the pruning block
func (b *Batch) IgnoreParents(block *block.Block) bool {
	return b.pruningBlock != nil && block.Domain.Header.DAAScore() < b.pruningBlock.Domain.Header.DAAScore()
}

// Add adds a pair `hash` and its matching `block` to the batch.
// Avoid duplicates and ignore blocks not in scope
func (b *Batch) Add(block *block.Block) {
	if !b.Has(block.Hash) && b.InScope(block) {
		block.IsPlaceholder = b.IgnoreParents(block)
		ba := &BlockAndHash{
			Block: block,
		}
		b.blocks = append(b.blocks, ba)
		b.hashes[*block.Hash] = ba
	}
}

func (b *Batch) TopologicalSort() []*BlockAndHash {
	var sorted = make([]*BlockAndHash, 0)
	var inDegree = make(map[externalapi.DomainHash]int, len(b.blocks))

	for _, ba := range b.blocks {
		ba.children = make(map[externalapi.DomainHash]*BlockAndHash)
		inDegree[*ba.Block.Hash] = 0
	}

	// Create children edges
	for _, ba := range b.blocks {
		for _, h := range ba.Domain.Header.DirectParents() {
			if parent, ok := b.hashes[*h]; ok {
				parent.children[*ba.Block.Hash] = ba
			}
		}
	}

	for _, ba := range b.blocks {
		for h, _ := range ba.children {
			if b.Has(&h) {
				inDegree[h]++
			}
		}
	}

	queue := queue.New()
	for h, degree := range inDegree {
		if degree == 0 {
			queue.PushBack(h)
		}
	}

	for !queue.IsEmpty() {
		current := queue.PopFront().(externalapi.DomainHash)
		ba := b.hashes[current]
		for h, _ := range ba.children {
			if b.Has(&h) {
				inDegree[h]--
				if inDegree[h] == 0 {
					queue.PushBack(h)
				}
			}
		}
		sorted = append(sorted, b.hashes[current])
	}

	if len(sorted) != len(b.blocks) {
		log.Errorf("Topological sort failed for DAG built on %s missing dependencies", b.blocks[0].Hash())
	}

	return sorted
}

// Has returns true if `hash` exists in the batch
func (b *Batch) Has(hash *externalapi.DomainHash) bool {
	_, ok := b.hashes[*hash]
	return ok
}

// Get returns the block identified by `hash`.
// Returns false if the hash is not found
func (b *Batch) Get(hash *externalapi.DomainHash) (*block.Block, bool) {
	value, ok := b.hashes[*hash]
	if !ok {
		return nil, false
	}
	return value.Block, true
}

func (b *Batch) Empty() bool {
	return len(b.blocks) == 0
}

// CollectBlockAndDependencies adds `block` and all its missing direct and
// indirect dependencies
func (b *Batch) CollectBlockAndDependencies(databaseTransaction *pg.Tx, block *block.Block) error {
	b.Add(block)
	i := 0
	for {
		item := b.blocks[i]
		err := b.collectDirectDependencies(databaseTransaction, item.Block)
		if err != nil {
			return err
		}
		i++
		if i >= len(b.blocks) {
			break
		}

		// If too many missing dependencies are found, just terminate the process and
		// let the service make a fresh restart.
		if len(b.blocks) > MaxSupportedMissingDependencies {
			log.Errorf("More then %d missing dependencies found! KGI is out of sync with the node.", MaxSupportedMissingDependencies)
			log.Errorf("Terminating the process so it can restart from scratch.")
			os.Exit(1)
		}
	}
	return nil
}

// collectDirectDependencies adds the missing direct parents of `block`
func (b *Batch) collectDirectDependencies(databaseTransaction *pg.Tx, child *block.Block) error {
	// Do not collect dependencies of blocks located below the pruning point
	if b.IgnoreParents(child) {
		log.Infof("Block %s is located below the pruning point so no further dependencies are collected", child.Hash)
		return nil
	}

	parentHashes := child.Domain.Header.DirectParents()
	for _, parentHash := range parentHashes {
		parentExists, err := b.database.DoesBlockExist(databaseTransaction, parentHash)
		if err != nil {
			// enhanced error description
			return errors.Wrapf(err, "Could not check if parent %s for block %s does exist in database", parentHash, child.Hash)
		}
		if !parentExists {
			rpcBlock, err := b.rpcClient.GetBlock(parentHash.String(), false)
			if err != nil {
				// We ignore the `block not found` kaspad error.
				// In this case the parent is out the node scope so we have no way
				// to include it in the batch
				log.Warnf("Parent %s for block %s not found by kaspad domain consensus; the missing dependency is ignored", parentHash, child.Hash)
				// TODO: Check that this is actually a not found error, and return error otherwise
			} else {
				parentBlock, err := block.GetBlockFromRpc(rpcBlock.Block)
				if err != nil {
					return err
				}
				b.Add(parentBlock)
				log.Warnf("Missing parent %s of %s registered for processing", parentHash, child.Hash)
			}
		}
	}
	return nil
}
