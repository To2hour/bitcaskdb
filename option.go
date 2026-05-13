package bitcaskdb

import (
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

//给db，batch用的初始化选项

type Options struct {
	// 打开的数据库路径
	DirPath string

	// wal中的一个seg的大小
	SegmentSize int64

	//是否在写的时候就刷盘
	Sync bool

	//// BytesPerSync specifies the number of bytes to write before calling fsync.
	//BytesPerSync uint32

	//// WatchQueueSize the cache length of the watch queue.
	//// if the size greater than 0, which means enable the watch.
	//WatchQueueSize uint64

	//	启用自动合并的定时任务，cron表达式的,为""则默认不启用自动合并
	AutoMergeCronExpr string
}

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

// DbDefaultOptions 给db默认用的选项
var DbDefaultOptions = &Options{
	DirPath:     "./example",
	SegmentSize: 1 * GB,
	Sync:        false,
	//每晚24点
	AutoMergeCronExpr: "0 0 * * *",
}

var nameRand = rand.NewSource(time.Now().UnixNano())

func tempDBDir() string {
	return filepath.Join(os.TempDir(), "rosedb-temp"+strconv.Itoa(int(nameRand.Int63())))
}
