package index

import (
	"bitcask-go/data"
	"bytes"
	"sort"
	"sync"

	"github.com/google/btree"
)

type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{key, pos}
	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}

func (bt *BTree) Delete(key []byte) bool {
	it := &Item{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	return oldItem != nil
}

func (bt *BTree) Size() int {
	return bt.tree.Len()
}

func (bt *BTree) Iterator(reverse bool) Iterator {
	return NewBTreeIterator(bt.tree, reverse)
}

func (bt *BTree) Close() error {
	return nil
}

// BTree 索引迭代器
type btreeIterator struct {
	index   int
	reverse bool
	items   []*Item
}

func NewBTreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	idx := 0
	items := make([]*Item, tree.Len())

	// 遍历索引将数据存放在数组中
	saveItem := func(it btree.Item) bool {
		items[idx] = it.(*Item)
		idx++
		return true
	}
	if reverse {
		tree.Descend(saveItem)
	} else {
		tree.Ascend(saveItem)
	}

	return &btreeIterator{
		index:   0,
		reverse: reverse,
		items:   items,
	}
}

func (it *btreeIterator) Rewind() {
	it.index = 0
}

func (it *btreeIterator) Seek(key []byte) {
	if it.reverse {
		it.index = sort.Search(len(it.items), func(i int) bool {
			return bytes.Compare(it.items[i].key, key) <= 0
		})
	} else {
		it.index = sort.Search(len(it.items), func(i int) bool {
			return bytes.Compare(it.items[i].key, key) >= 0
		})
	}
}

func (it *btreeIterator) Next() {
	it.index += 1
}

func (it *btreeIterator) Valid() bool {
	return it.index < len(it.items)
}

func (it *btreeIterator) Key() []byte {
	return it.items[it.index].key
}

func (it *btreeIterator) Value() *data.LogRecordPos {
	return it.items[it.index].pos
}

func (it *btreeIterator) Close() {
	it.items = nil
}
