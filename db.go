package bitcaskdb

import (
	"bitcaskdb/index"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"

	"github.com/gofrs/flock"
	"github.com/robfig/cron/v3"
	"github.com/rosedblabs/wal"
)

type DB struct {
	// data files are a sets of segment files in WAL.
	dataFiles *wal.WAL
	// hint file is used to store the key and the position for fast startup.
	hintFile *wal.WAL

	//初始化的选项
	options *Options
	// 再commit的时候需要调用wal的write，write会返回一个ChunkPosition对象
	// 需要以一种数据结构把这个kv结构存下来。map本身开销太大，同时无序没法支持like
	// 所以用b树
	index index.Indexer
	//options          Options
	//这个锁是用来保证该目录下的数据库只能被一个db所持有
	fileLock *flock.Flock
	//数据库在关闭后就不能用了，但没办法组织写代码的时候接着硬用，只能多加1层close的校验了
	closed       bool
	mergeRunning uint32 // indicate if the database is merging

	//一个db可以同时获取多个batch，然后每个batch依然可以并发
	//所以针对commit，rollback操作需要获取db的mu锁，get，put这种获取batch自己的锁就行
	batchPool sync.Pool
	mu        sync.RWMutex
	//在batch的put的中，装最基础数据,同样是避免频繁make
	baseDataStructPool sync.Pool

	// 一个常驻的切片，用于在baseData加密中提供一个容器，避免频繁make
	// 不放batch里是因为加密仅仅在commit的时候会用到，而commit的时候加锁了
	encodeHeader []byte
	//watchCh          chan *Event // user consume channel for watch events
	//watcher          *Watcher
	expiredCursorKey []byte     // the location to which DeleteExpiredKeys executes.
	cronScheduler    *cron.Cron // cron scheduler for auto merge task
}

// type batchId expire keySize valueSize
//
//	1  +  10  +   10   +   10   +    10  = 41
const maxBaseDataHeaderSize = 1 + binary.MaxVarintLen64*4

const (
	//锁的后缀
	fileLockName = "FLOCK"
	//数据文件的后缀
	dataFileNameSuffix = ".SEG"
	// 索引文件的后缀
	hintFileNameSuffix = ".HINT"
	// 合并文件的后缀
	mergeFinNameSuffix = ".MERGEFIN"
)

func Open(option *Options) (*DB, error) {
	if err := checkOptions(option); err != nil {
		return nil, err
	}

	//如果目录不存在就创建
	if _, err := os.Stat(option.DirPath); err != nil {
		if err := os.MkdirAll(option.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	//todo open的流程：
	// 初始化fileLock锁(在数据目录下新建一个叫flock的文件就行)并尝试获取，失败说明有人在用直接报错
	fileLock := flock.New(filepath.Join(option.DirPath, fileLockName))
	lockResult, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !lockResult {
		return nil, ErrDatabaseIsUsing
	}
	// 初始化db
	db := &DB{
		index:              index.NewIndexer(),
		options:            option,
		fileLock:           fileLock,
		batchPool:          sync.Pool{New: newBatch},
		baseDataStructPool: sync.Pool{New: newBaseDataStruct},
		encodeHeader:       make([]byte, maxBaseDataHeaderSize),
	}

	// 打开wal
	if db.dataFiles, err = openDataWal(option); err != nil {
		return nil, err
	}

	// 加载索引
	//todo 我的思路是：把indexer用同样的wal存起来。第一个val放编号(我认为可以直接用当前的activeSegment)
	// 然后从wal加载进来后，根据编号去加载后面的
	// 不过首先得做到遍历indexer
	// 所以下午我需要把indexer的Iterator弄了

	// 解析cron表达式并创建定时任务

	return db, nil
}

// Close todo 把各种数据结构清空。文件句柄还回去,锁释放
func (db *DB) Close() {

}

// Put todo 暂时先byte，未来改成any
func (db *DB) Put(key, value []byte) error {
	batch := db.batchPool.Get().(*Batch)
	//把batch清空，否则有问题
	defer db.batchPool.Put(batch)
	defer batch.reset()
	batch.init(false, db)
	batch.Lock()
	if err := batch.Put(key, value); err != nil {
		return err
	}
	if err := batch.Commit(); err != nil {
		return err
	}
	return nil
}
func (db *DB) Get(key []byte) ([]byte, error) {
	batch := db.batchPool.Get().(*Batch)
	//把batch清空，否则有问题
	defer db.batchPool.Put(batch)
	defer batch.reset()
	batch.init(true, db)
	batch.Lock()
	res, err := batch.Get(key)
	if err != nil {
		return nil, err
	}
	return res, nil
}
func (db *DB) Delete(key []byte) error {
	batch := db.batchPool.Get().(*Batch)
	//把batch清空，否则有问题
	defer db.batchPool.Put(batch)
	defer batch.reset()
	batch.init(false, db)
	batch.Lock()
	if err := batch.Delete(key); err != nil {
		return err
	}
	if err := batch.Commit(); err != nil {
		return err
	}
	return nil
}

// openDataWal 打开数据的wal
func openDataWal(option *Options) (*wal.WAL, error) {
	open, err := wal.Open(wal.Options{
		DirPath:        option.DirPath,
		SegmentSize:    option.SegmentSize,
		SegmentFileExt: dataFileNameSuffix,
		//下面这两个不知道干嘛的，先不管
		Sync:         false,
		BytesPerSync: 0,
	})
	return open, err
}

// 检查db的option是否合法
func checkOptions(option *Options) error {
	if option.DirPath == "" {
		return errors.New("数据库路径不能为空")
	}
	if option.SegmentSize <= 0 {
		return errors.New("seg段不能为0")
	}
	//todo cron晚点检查
	return nil
}

// 给db的batchPool初始化用的，batch如果还想使用需要init
func newBatch() any {
	return &Batch{}
}

// 给db的baseDataStructPool初始化用的
func newBaseDataStruct() any {
	return &baseDataStruct{}
}

func (db *DB) NewBatch() *Batch {
	b := &Batch{
		db:        db,
		mu:        sync.RWMutex{},
		committed: false,
	}
	return b
}
