package index

import (
	"bytes"
	"sync"

	"github.com/google/btree"
	"github.com/rosedblabs/wal"
)

// BTreeIndexer 给tree加个锁更好
type BTreeIndexer struct {
	tree *btree.BTree
	mu   *sync.RWMutex
}

// BTreeItem 用的btree的数据接口底层只支持一个item对象，所以需要把这两套一下
type BTreeItem struct {
	key      []byte
	position *wal.ChunkPosition
}

func (bit *BTreeItem) Less(than btree.Item) bool {
	item := than.(*BTreeItem)
	return bytes.Compare(bit.key, item.key) < 0
}

func (bit *BTreeIndexer) Put(key []byte, position *wal.ChunkPosition) *wal.ChunkPosition {
	bit.mu.Lock()
	defer bit.mu.Unlock()
	oldBTreeItem := bit.tree.ReplaceOrInsert(&BTreeItem{
		key:      key,
		position: position,
	})
	if oldBTreeItem == nil {
		return nil
	}
	return oldBTreeItem.(*BTreeItem).position
}

func (bit *BTreeIndexer) Get(key []byte) *wal.ChunkPosition {
	bit.mu.RLock()
	defer bit.mu.RUnlock()
	value := bit.tree.Get(&BTreeItem{
		key: key,
	})
	if value != nil {
		return value.(*BTreeItem).position
	}
	return nil
}

func (bit *BTreeIndexer) Delete(key []byte) *wal.ChunkPosition {
	bit.mu.RLock()
	defer bit.mu.RUnlock()
	value := bit.tree.Delete(&BTreeItem{
		key: key,
	})
	if value != nil {
		return value.(*BTreeItem).position
	}
	return nil
}

// Iterator 我需要返回一个IteratorXX对象,这个对象应该得有next方法
func (bit *BTreeIndexer) Iterator(reverse bool) IndexerIterator {
	current := func() *BTreeItem {
		if reverse {
			return bit.tree.Max().(*BTreeItem)
		}
		return bit.tree.Min().(*BTreeItem)
	}()
	return &BtreeIterator{
		current: current,
		reverse: reverse,
		valid:   false,
		tree:    bit.tree.Clone(),
	}
}

func (bit *BTreeIndexer) Size() int {
	return bit.tree.Len()
}

// 这里返回的对象最好带读写锁
func newBTree() *BTreeIndexer {
	return &BTreeIndexer{
		tree: btree.New(32),
		mu:   new(sync.RWMutex),
	}
}
