package bitcaskdb

import (
	"sync"

	"github.com/gofrs/flock"
	"github.com/robfig/cron/v3"
	"github.com/rosedblabs/wal"
)

type DB struct {
	dataFiles *wal.WAL // data files are a sets of segment files in WAL.
	hintFile  *wal.WAL // hint file is used to store the key and the position for fast startup.
	//index            index.Indexer
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
