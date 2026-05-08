package bitcaskdb

import (
	"bitcaskdb/util"
	"bytes"
	"sync"

	"github.com/valyala/bytebufferpool"
)

// Batch 作用：弄一个数组baseData，然后commit的时候一下写完。然后实现get put commit delete
type Batch struct {
	//待写入的数据
	pendingBaseData []*baseDataStruct
	db              *DB
	//用来存pendingBaseData的map，key是uint，
	//因为这里直接调用hash算，value是切片是可能hash碰撞,value存的是pendingBaseData的某个对象的下标
	pendingBaseDataMap map[uint64][]int
	mu                 sync.RWMutex
	committed          bool
	//是否回滚
	rollback bool
	//这个是优化手段，在commit的时候需要加密，加密需要用到一个缓冲的[]byte，所以用池管理下避免频繁make
	buffers []*bytebufferpool.ByteBuffer
	//todo是否只读(有必要加吗？先看看)
	//有的，一个db可以有很多batch，commit，rollback操作只能有写batch操作，
	// 只读的batch可以仅仅获取db的r锁，而写的batch必须获取写锁才行
	//然后同一个batch依然可能由多个协程操作，所以batch本身也需要锁
	// true是只读，false是写batch
	readOnly bool
}

func (b *Batch) Lock() {
	if b.readOnly {
		b.db.mu.RLock()
	} else {
		b.db.mu.Lock()
	}
}
func (b *Batch) Unlock() {
	if b.readOnly {
		b.db.mu.RUnlock()
	} else {
		b.db.mu.Unlock()
	}
}

// Put 读,写batch都行，用batch本身的锁就行
func (b *Batch) Put(key, value []byte) error {
	if b.committed {
		return ErrBatchCommitted
	}
	//先检查map里有没有
	b.mu.Lock()
	defer b.mu.Unlock()
	data := b.checkPendingBaseData(key)
	if data != nil {
		data.Key = key
		data.Value = value
		data.Type = Normal
		data.Expire = 0
		return nil
	}
	dataStruct := b.db.baseDataStructPool.Get().(*baseDataStruct)
	b.appendPendingBaseData(key, dataStruct)
	dataStruct.Key, dataStruct.Value = key, value
	dataStruct.Type, dataStruct.Expire = Normal, 0
	return nil
}

// Delete 同put
func (b *Batch) Delete(key []byte) error {
	if b.committed {
		return ErrBatchCommitted
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	data := b.checkPendingBaseData(key)
	if data != nil {
		data.Key = key
		data.Value = nil
		data.Type = Deleted
		data.Expire = 0
		return nil
	}
	dataStruct := b.db.baseDataStructPool.Get().(*baseDataStruct)
	b.appendPendingBaseData(key, dataStruct)
	dataStruct.Key, dataStruct.Value = key, nil
	dataStruct.Type, dataStruct.Expire = Deleted, 0
	return nil
}
func (b *Batch) Commit() error {
	//commit完了这个batch就直接不要了，所以得把锁还了
	defer b.mu.Unlock()
	if b.readOnly {
		return ErrReadOnlyBatch
	}

	b.mu.Lock()
	defer b.Unlock()
	batchId := util.GenerateBatchId()
	//todo 先检查合法性

	// 然后把pendingBaseData里的数据用encodeBaseDataStruct加密成byte，然后用wal.PendingWrites写进去
	for _, baseData := range b.pendingBaseData {
		buf := bytebufferpool.Get()
		b.buffers = append(b.buffers, buf)
		baseData.BatchId = uint64(batchId)
		res := encodeBaseDataStruct(buf, b.db.encodeHeader, baseData)
		b.db.dataFiles.PendingWrites(res)
	}
	// 然后制造一个完成的数据同样加密并放进去
	buf := bytebufferpool.Get()
	b.buffers = append(b.buffers, buf)
	end := encodeBaseDataStruct(buf, b.db.encodeHeader, &baseDataStruct{
		Key:     batchId.Bytes(),
		Type:    Finished,
		BatchId: uint64(batchId),
	})
	b.db.dataFiles.PendingWrites(end)
	// wal的write会返回一个ChunkPosition对象。查询的时候需要传入这个对象才行
	// 所以得在db或者batch里维护一个数据结构(indexer)，
	// 不用map是因为map 1.无序 2. 复制开销太大
	// value放ChunkPosition
	posList, err := b.db.dataFiles.WriteAll()
	if err != nil {
		//写入失败后，把wal里的缓冲区清空
		b.db.dataFiles.ClearPendingWrites()
		return err
	}
	//遍历目前的 b.pendingBaseData，把写入的给更新到indexer中
	for i, baseData := range b.pendingBaseData {
		if baseData.Type == Deleted {
			b.db.index.Delete(baseData.Key)
		} else {
			//返回的pos顺序应当和写入的时候的pendingBaseData顺序一致
			b.db.index.Put(baseData.Key, posList[i])
		}
		//put和delete的对象是从recordPool get的，用完需要还回去
		b.db.baseDataStructPool.Put(baseData)
	}
	b.committed = true
	//todo 为了方便，暂时先清空pending
	b.pendingBaseData = b.pendingBaseData[:0]
	b.pendingBaseDataMap = nil
	return nil
}
func (b *Batch) Get(key []byte) ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	data := b.checkPendingBaseData(key)
	if data != nil {
		return data.Value, nil
	}
	chunkPosition := b.db.index.Get(key)
	val, err := b.db.dataFiles.Read(chunkPosition)
	if err != nil {
		return nil, err
	}
	data = decodeBaseDataStruct(val)
	if data == nil {
		panic("很奇怪的问题发生了")
	}
	return data.Value, nil
}

// 检查key是不是还在内存没提交
func (b *Batch) checkPendingBaseData(key []byte) *baseDataStruct {
	//先检查map里有没有
	hashkey := util.ByteHash(key)
	//如果有就遍历
	for _, value := range b.pendingBaseDataMap[hashkey] {
		if bytes.Equal(b.pendingBaseData[value].Key, key) {
			return b.pendingBaseData[value]
		}
	}
	return nil
}

func (b *Batch) appendPendingBaseData(key []byte, dataStruct *baseDataStruct) {
	//把baseDataStruct对象的指针先放到pendingBaseData中。
	b.pendingBaseData = append(b.pendingBaseData, dataStruct)
	if b.pendingBaseDataMap == nil {
		b.pendingBaseDataMap = make(map[uint64][]int)
	}
	hashKey := util.ByteHash(key)
	//然后更新map，len(b.pendingBaseData)-1是最后一个下标也就是刚才新加的指针的下标
	b.pendingBaseDataMap[hashKey] = append(b.pendingBaseDataMap[hashKey], len(b.pendingBaseData)-1)
}

// Rollback 回滚，用于给db的put，delete的，作用是清空pendingWrite
// todo 可能db会涉及同时拥有多个batch，先观望
func (b *Batch) Rollback() error {
	defer b.Lock()
	if b.readOnly {
		return ErrReadOnlyBatch
	}
	if b.committed {
		return ErrBatchCommitted
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	// buffer是在编码基础数据的时候用到的，需要清空
	for _, buffer := range b.buffers {
		bytebufferpool.Put(buffer)
	}
	// pendingBaseData里的数据是从recordPool里借的，需要还回去
	for _, data := range b.pendingBaseData {
		b.db.baseDataStructPool.Put(data)
	}
	//把pendingBaseData和pendingBaseDataMap清空
	b.pendingBaseData = b.pendingBaseData[:0]
	for key := range b.pendingBaseDataMap {
		delete(b.pendingBaseDataMap, key)
	}
	return nil
}
