package model

const (
	ColorGray = "gray"
	ColorRed  = "red"
	ColorBlue = "blue"
)

type Block struct {
	ID        uint64   `pg:"id,pk"`
	BlockHash string   `pg:"block_hash"`
	Timestamp int64    `pg:"timestamp,use_zero"`
	ParentIDs []uint64 `pg:"parent_ids,use_zero"`
	Height    uint64   `pg:"height,use_zero"`
	Color     string   `pg:"color"`
}
