package index

import (
	"bitcask-go/data"
	"bytes"
	"sort"
	"sync"

	goart "github.com/plar/go-adaptive-radix-tree"
)

type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: new(sync.RWMutex),
	}
}

func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) bool {
	art.lock.Lock()
	art.tree.Insert(key, pos)
	art.lock.Unlock()
	return true
}

func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()
	v, found := art.tree.Search(key)
	if !found {
		return nil
	}
	return v.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Delete(key []byte) bool {
	art.lock.Lock()
	_, ok := art.tree.Delete(key)
	art.lock.Unlock()
	return ok
}

func (art *AdaptiveRadixTree) Size() int {
	art.lock.RLock()
	size := art.tree.Size()
	art.lock.RUnlock()
	return size
}

func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	return NewARTIterator(art.tree, reverse)
}

func (art *AdaptiveRadixTree) Close() error {
	return nil
}

// ART 索引迭代器
type artIterator struct {
	index   int
	reverse bool
	items   []*Item
}

func NewARTIterator(tree goart.Tree, reverse bool) *artIterator {
	idx := 0
	if reverse {
		idx = tree.Size() - 1
	}
	items := make([]*Item, tree.Size())

	// 遍历索引将数据存放在数组中
	saveItem := func(node goart.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		items[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}

	tree.ForEach(saveItem)

	return &artIterator{
		index:   0,
		reverse: reverse,
		items:   items,
	}
}

func (it *artIterator) Rewind() {
	it.index = 0
}

func (it *artIterator) Seek(key []byte) {
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

func (it *artIterator) Next() {
	it.index += 1
}

func (it *artIterator) Valid() bool {
	return it.index < len(it.items)
}

func (it *artIterator) Key() []byte {
	return it.items[it.index].key
}

func (it *artIterator) Value() *data.LogRecordPos {
	return it.items[it.index].pos
}

func (it *artIterator) Close() {
	it.items = nil
}
