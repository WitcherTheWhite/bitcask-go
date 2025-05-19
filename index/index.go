package index

import (
	"bitcask-go/data"
	"bytes"

	"github.com/google/btree"
)

// 抽象索引接口，可以支持不同的数据结构
type Indexer interface {
	// 在索引中存储 key 对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) bool

	// 根据 key 获取对应的数据位置信息
	Get(key []byte) *data.LogRecordPos

	// 根据 key 删除对应的数据位置信息
	Delete(key []byte) bool
}

type IndexType byte

const (
	BTree IndexType = iota
)

// 初始化内存索引
func NewIndexer(typ IndexType) Indexer {
	switch typ {
	case BTree:
		return NewBtree()
	default:
		panic("unsupported index type")
	}

}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}
