package bitcaskdb

import (
	"bitcaskdb/index"
	"sync"

	"github.com/gofrs/flock"
	"github.com/robfig/cron/v3"
	"github.com/rosedblabs/wal"
)

type DB struct {
	dataFiles *wal.WAL // data files are a sets of segment files in WAL.
	hintFile  *wal.WAL // hint file is used to store the key and the position for fast startup.

	// 再commit的时候需要调用wal的write，write会返回一个ChunkPosition对象
	// 需要以一种数据结构把这个kv结构存下来。map本身开销太大，同时无序没法支持like
	// 所以用b树
	index index.Indexer
	//options          Options
	fileLock     *flock.Flock
	mu           sync.RWMutex
	closed       bool
	mergeRunning uint32 // indicate if the database is merging
	batchPool    sync.Pool
	recordPool   sync.Pool
	encodeHeader []byte
	//watchCh          chan *Event // user consume channel for watch events
	//watcher          *Watcher
	expiredCursorKey []byte     // the location to which DeleteExpiredKeys executes.
	cronScheduler    *cron.Cron // cron scheduler for auto merge task
}
