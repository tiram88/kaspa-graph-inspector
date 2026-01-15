package block

import (
	"github.com/kaspa-live/kaspa-graph-inspector/processing/infrastructure/network/rpcclient"
	"github.com/kaspanet/kaspad/app/appmessage"
	"github.com/kaspanet/kaspad/domain/consensus/model/externalapi"
	"github.com/kaspanet/kaspad/domain/consensus/utils/consensushashing"
)

type Block struct {
	Hash          *externalapi.DomainHash
	Domain        *externalapi.DomainBlock
	Rpc           *appmessage.RPCBlock
	IsPlaceholder bool
}

func GetBlockFromRpc(rpc *appmessage.RPCBlock) (*Block, error) {
	domain, err := appmessage.RPCBlockToDomainBlock(rpc)
	if err != nil {
		return nil, err
	}

	block := &Block{
		Hash:   consensushashing.BlockHash(domain),
		Domain: domain,
		Rpc:    rpc,
	}
	return block, nil
}

func GetBlockFromString(rpcClient *rpcclient.RPCClient, stringHash string) (*Block, error) {
	hash, err := externalapi.NewDomainHashFromString(stringHash)
	if err != nil {
		return nil, err
	}

	block, err := GetBlock(rpcClient, hash)
	if err != nil {
		return nil, err
	}
	return block, nil
}

func GetBlock(rpcClient *rpcclient.RPCClient, hash *externalapi.DomainHash) (*Block, error) {
	rpc, err := rpcClient.GetBlock(hash.String(), false)
	if err != nil {
		return nil, err
	}

	domain, err := appmessage.RPCBlockToDomainBlock(rpc.Block)
	if err != nil {
		return nil, err
	}

	block := &Block{
		Hash:   hash,
		Domain: domain,
		Rpc:    rpc.Block,
	}
	return block, nil
}
