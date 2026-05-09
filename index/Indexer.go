package index

import "github.com/rosedblabs/wal"

// Indexer 再commit的时候需要调用wal的write，write会返回一个ChunkPosition对象
// 需要Indexer把这个kv结构存下来。
// Indexer可以再IndexerType自由选择，建议用tree
// 因为map本身开销太大，同时无序没法支持like
type Indexer interface {
	// Put 传入k，v。如果不存在返回nil，如果老的存在返回老的v
	Put(key []byte, position *wal.ChunkPosition) *wal.ChunkPosition

	Get(key []byte) *wal.ChunkPosition

	//	Delete 如果没有返回nil。否则返回老的v
	Delete(key []byte) *wal.ChunkPosition

	// Iterator 迭代器，用来遍历树
	Iterator(reverse bool) IndexerIterator

	Size() int
}

// IndexerIterator 迭代器
type IndexerIterator interface {
	// Rewind 重置迭代器，从0开始
	Rewind()

	// Seek 跳转到key的位置
	Seek(key []byte)

	// Next 移动到下一个位置
	Next()

	// Valid 检查是否合法
	Valid() bool

	// Key 返回当前元素的key
	Key() []byte

	// Value 返回当前元素的value
	Value() *wal.ChunkPosition

	// Close 关闭这个迭代器
	Close()
}

// IndexerType 索引类型
type IndexerType byte

const (
	BTree IndexerType = iota
)

var indexType = BTree

func NewIndexer() Indexer {
	switch indexType {
	case BTree:
		return newBTree()
	default:
		panic("unexpected index type")
	}
}
