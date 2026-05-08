package index

import (
	"github.com/google/btree"
	"github.com/rosedblabs/wal"
)

type BtreeIterator struct {
	//当前btreeItem对象
	current *BTreeItem
	//是否反转
	reverse bool
	//当前cur是否合法
	valid bool
	//用来操作的树
	tree *btree.BTree
}

func (b *BtreeIterator) Rewind() {
	if b.tree == nil {
		return
	}
	if b.reverse {
		//倒序遍历，返回false的时候就听了，所以相当于返回最后一个
		b.tree.Descend(func(item btree.Item) bool {
			b.current = item.(*BTreeItem)
			return false
		})
	} else {
		//正序遍历
		b.tree.Ascend(func(item btree.Item) bool {
			b.current = item.(*BTreeItem)
			return false
		})
	}
	b.valid = true
}

func (b *BtreeIterator) Seek(key []byte) {
	if !b.valid {
		return
	}
	seekItem := &BTreeItem{key: key}
	b.valid = false

	if b.reverse {
		// DescendLessOrEqual作用是：从[first,seek]开始遍历
		// 注意：DescendLessOrEqual会先找到满足seekItem的数据，然后在进入func，所以直接保存即可
		b.tree.DescendLessOrEqual(seekItem, func(item btree.Item) bool {
			b.current = item.(*BTreeItem)
			b.valid = true
			return false
		})
	} else {
		// 从[seek,end]开始，其他同DescendLessOrEqual
		b.tree.AscendGreaterOrEqual(seekItem, func(item btree.Item) bool {
			b.current = item.(*BTreeItem)
			b.valid = true
			return false
		})
	}
}

func (b *BtreeIterator) Next() {
	if !b.valid {
		return
	}
	b.valid = false
	if b.reverse {
		b.tree.DescendLessOrEqual(b.current, func(item btree.Item) bool {
			//DescendLessOrEqual是从[current,first]的顺序的,包括current
			// 所以current == key的时候说明还没动，直接下一个
			// 不用bytes.Equal(item.(*BTreeItem).key, b.current.key)
			// 是因为可能item的key变成了any，不过意义不大。变成any整个都得重写
			// 不差这个迭代器，但总归比硬编码好一点
			if !b.current.Less(item) {
				return true
			}
			b.valid = true
			b.current = item.(*BTreeItem)
			return false
		})
	} else {
		//正序遍历
		b.tree.AscendGreaterOrEqual(b.current, func(item btree.Item) bool {
			//AscendGreaterOrEqual[current,end]的顺序的,包括current
			// 所以current == key的时候说明还没动，直接下一个
			if !b.current.Less(item) {
				return true
			}
			b.valid = true
			b.current = item.(*BTreeItem)
			return false
		})
	}
}

func (b *BtreeIterator) Valid() bool {
	return b.valid
}

func (b *BtreeIterator) Key() []byte {
	return b.current.key
}

func (b *BtreeIterator) Value() *wal.ChunkPosition {
	return b.current.position
}

func (b *BtreeIterator) Close() {
	b.tree.Clear(true)
	b.tree = nil
	b.current = nil
	b.valid = false
}
