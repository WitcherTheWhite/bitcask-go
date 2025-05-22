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

	// 索引中 key 的数量
	Size() int

	// 返回迭代器用于有序的遍历所有数据
	Iterator(reverse bool) Iterator
}

type IndexType byte

const (
	Btree IndexType = iota
)

// 初始化内存索引
func NewIndexer(typ IndexType) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
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

// 通用索引迭代器
type Iterator interface {
	// 重新回到迭代器的起点，即第一个数据
	Rewind()

	// 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
	Seek(key []byte)

	// 跳转到下一个 key
	Next()

	// 是否有效，即是否已经遍历完了所有的 key，用于退出遍历
	Valid() bool

	// 当前遍历位置的 Key 数据
	Key() []byte

	// 当前遍历位置的 Value 数据
	Value() *data.LogRecordPos

	// 关闭迭代器，释放相应资源
	Close()
}
