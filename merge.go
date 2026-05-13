package bitcaskdb

import (
	"bitcaskdb/index"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/rosedblabs/wal"
	"github.com/valyala/bytebufferpool"
)

const (
	mergeDirSuffixName   = "-Merge"
	mergeFinishedBatchID = 0
)

// Merge 把wal中那些老的seg段合并成更少个
// 从wal中new reader。当遍历到的seg == active的时候就停止
// 然后把reader读到的数据解码，类似loadIndex后
// put到索引里，然后索引猛猛put后，检测下大小，差不多
// 和seg设定的大小一样了就commit进去，不过要不然改commit，要不然写个新的commit
// 问题是怎么覆盖原文件？然后怎么替换？

//	roseDb思路：创造一个临时文件夹，把目前active的seg往后一位，然后read除了新的之外所有的seg
//
// 然后根据delete，过期时间等标记只把有用的数据放到新的seg中
// 然后用rename替换掉老的seg(遍历新的seg然后直接改名过去替换即可)
// 然后替换结束后重新加载索引，就ok了
func (db *DB) Merge() error {
	//不管这么多，先doMerge把文件创建出来
	if err := db.DoMerge(); err != nil {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	//把当前的data给关闭掉
	if err := db.closeFiles(); err != nil {
		return err
	}
	//直接把现在db下的data文件替换成mergeDB的data文件，直接rename改
	//替换原数据文件
	err := ReplaceOriginalFile(db.options.DirPath)
	if err != nil {
		return err
	}
	//替换好了，重新打开
	dataWal, err := openDataWal(db.options)
	if err != nil {
		return err
	}
	db.dataFiles = dataWal
	//然后重新加载索引
	db.index = index.NewIndexer()
	if err = db.loadIndex(); err != nil {
		return err
	}
	return nil
}

func ReplaceOriginalFile(path string) error {
	mergePath := mergeDirPath(path)
	//不存在就直接返回
	if _, err := os.Stat(mergePath); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	//合并完后就没用了，直接删了就行
	defer func() {
		_ = os.RemoveAll(mergePath)
	}()
	// force:如果文件的大小是0，为true的时候也创建，否则不创建
	// 一个优化手段
	ReplaceFile := func(suffix string, segId uint32, force bool) {
		//现在merge的位置：
		mergeFile := wal.SegmentFileName(mergePath, suffix, segId)
		stat, err := os.Stat(mergeFile)
		//因为遍历到的是finSegmentId，所以可能出现old有这个编号但seg没有的。
		//这种情况直接返回
		if os.IsNotExist(err) {
			return
		}
		if err != nil {
			panic(fmt.Sprintf("loadMergeFiles: failed to get src file stat %v", err))
		}
		if !force && stat.Size() == 0 {
			return
		}
		//merge文件目标的位置
		srcFile := wal.SegmentFileName(path, suffix, segId)
		//把老的干掉换成merge里的
		_ = os.Rename(mergeFile, srcFile)
	}
	//读取doMerge时生成的mergeFinNameSuffix，里面记录了最后一个data的索引
	finSegmentId, err := getMergeFinSegmentId(mergePath)
	if finSegmentId == 0 {
		return errors.New("the last Merge was interrupted by an exception, please Merge again")
	}
	if err != nil {
		return err
	}
	// 先删掉旧的索引标识文件
	// 只有删除了这两个，即便后面 Rename 过程中断了，
	// 重启时 loadIndexFromHintFile 也会因为找不到 fin 文件而返回 nil，
	// 从而迫使数据库执行安全的全量数据扫描。
	_ = os.Remove(wal.SegmentFileName(path, mergeFinNameSuffix, 1))
	_ = os.Remove(wal.SegmentFileName(path, hintFileNameSuffix, 1))
	for i := uint32(1); i <= finSegmentId; i++ {
		//把老的删掉，因为虽然rename会把老的干掉，但merge的seg如果比old的更少的话
		//就会遗留old
		oldFile := wal.SegmentFileName(path, dataFileNameSuffix, i)
		if _, err = os.Stat(oldFile); err == nil {
			if err = os.Remove(oldFile); err != nil {
				return err
			}
		}
		ReplaceFile(dataFileNameSuffix, i, false)
	}
	//把hint和merge给移过去
	ReplaceFile(hintFileNameSuffix, 1, true)
	ReplaceFile(mergeFinNameSuffix, 1, true)
	return nil
}

// DoMerge 读取老的seg并在新目录创建新的data文件
func (db *DB) DoMerge() error {
	//开局先上锁
	db.mu.Lock()
	//先验证合法性
	if db.closed {
		db.mu.Unlock()
		return ErrDBClosed
	}
	if db.dataFiles == nil {
		db.mu.Unlock()
		return ErrDataFileNil
	}
	//LoadUint32类似Java更改violate值
	if atomic.LoadUint32(&db.mergeRunning) == 1 {
		db.mu.Unlock()
		return ErrMergeRunning
	}
	//锁住
	atomic.StoreUint32(&db.mergeRunning, 1)
	defer atomic.StoreUint32(&db.mergeRunning, 0)
	//把wal的活跃activeSeg改了
	preActiveSegmentID := db.dataFiles.ActiveSegmentID()
	if err := db.dataFiles.OpenNewActiveSegment(); err != nil {
		db.mu.Unlock()
		return err
	}
	//把锁放了，现在写的都在新的seg里，接下来只操作老的了
	db.mu.Unlock()
	//然后找个新文件夹，打开merge的db
	//newMergeDB会把merge文件夹下的所有东西删了，避免上一次影响
	mergeDB, err := db.newMergeDB()
	if err != nil {
		return err
	}
	defer mergeDB.Close()
	//迭代器只迭代到preActiveSegmentID为止
	reader := db.dataFiles.NewReaderWithMax(preActiveSegmentID)
	//buf,给encodeBaseDataStruct用的
	now := time.Now().UnixNano()
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	for {
		val, position, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		//解码数据
		dataStruct := decodeBaseDataStruct(val)
		if dataStruct == nil {
			return errors.New("正常不会出现的错误")
		}
		//如果解码数据的pos == index的pos，说明他正在用，保留，否则丢弃
		//因为delete后，index已经把这个删掉了，所以理论上
		//正确维护的index里面一定没有delete，所以没必要保留文件里的delete
		if dataStruct.Type == Normal && (dataStruct.Expire == 0 || dataStruct.Expire > now) {
			db.mu.RLock()
			indexPos := db.index.Get(dataStruct.Key)
			db.mu.RUnlock()
			if indexPos != nil && positionEquals(indexPos, position) {
				dataStruct.BatchId = mergeFinishedBatchID
				//把有用的数据写进来
				write, err := mergeDB.dataFiles.Write(encodeBaseDataStruct(buf, mergeDB.encodeHeader, dataStruct))
				if err != nil {
					return err
				}
				// 把key和pos数据存到hint里
				_, err = mergeDB.hintFile.Write(encodeHintRecord(dataStruct.Key, write))
				if err != nil {
					return err
				}
			}
		}
	}
	//数据都写进datafile和hintFile了。然后我们需要一个mergeFile
	//用来标识这次merge完成了。如果没这个文件我们就得重新merge
	mergeFinish, err := wal.Open(wal.Options{
		DirPath:        mergeDB.options.DirPath,
		SegmentSize:    GB,
		SegmentFileExt: mergeFinNameSuffix,
		Sync:           false,
		BytesPerSync:   0,
	})
	if err != nil {
		return err
	}
	defer func() {
		_ = mergeFinish.Close()
	}()
	_, err = mergeFinish.Write(encodeMergeFinish(preActiveSegmentID))
	if err != nil {
		return err
	}
	return nil
}
func positionEquals(a, b *wal.ChunkPosition) bool {
	return a.SegmentId == b.SegmentId &&
		a.BlockNumber == b.BlockNumber &&
		a.ChunkOffset == b.ChunkOffset
}
func (db *DB) newMergeDB() (*DB, error) {
	//计算出merge文件夹的位置
	mergePath := mergeDirPath(db.options.DirPath)
	//把老的merge文件夹清空
	if err := os.RemoveAll(mergePath); err != nil {
		return nil, err
	}
	mergeOptionsCopy := *db.options
	mergeOptionsCopy.DirPath = mergePath
	//创建新的临时文件目录
	mergeDB, err := Open(&mergeOptionsCopy)
	if err != nil {
		return nil, err
	}
	hintFile, err := wal.Open(wal.Options{
		DirPath: mergeOptionsCopy.DirPath,
		// hint文件不需要分段
		SegmentSize:    math.MaxInt64,
		SegmentFileExt: hintFileNameSuffix,
		Sync:           false,
		BytesPerSync:   0,
	})
	if err != nil {
		return nil, err
	}
	mergeDB.hintFile = hintFile
	return mergeDB, nil
}
func mergeDirPath(dirPath string) string {
	dir := filepath.Dir(filepath.Clean(dirPath))
	base := filepath.Base(dirPath)
	return filepath.Join(dir, base+mergeDirSuffixName)
}
func (db *DB) closeFiles() error {
	// close wal
	if err := db.dataFiles.Close(); err != nil {
		return err
	}
	// close hint file if exists
	if db.hintFile != nil {
		if err := db.hintFile.Close(); err != nil {
			return err
		}
	}
	return nil
}

//todo 到时候写完merge，写一个基于lru的内存淘汰机制，让indexer别保存全量索引
