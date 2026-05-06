package bitcaskdb

import (
	"bitcaskdb/util"
	"bytes"
	"sync"
)

// 作用：弄一个数组baseData，然后commit的时候一下写完。然后实现get put commit delete
type batch struct {
	//待写入的数据
	pendingBaseData []*baseDataStruct
	db              *DB
	//用来存pendingBaseData的map，key是uint，
	//因为这里直接调用hash算，value是切片是可能hash碰撞,value存的是pendingBaseData的某个对象的下标
	pendingBaseDataMap map[uint64][]int
	mu                 sync.RWMutex
	committed          bool
}

func (b *batch) Put(key, value []byte) error {
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
	dataStruct := b.db.recordPool.Get().(*baseDataStruct)
	b.appendPendingBaseData(key, dataStruct)
	dataStruct.Key, dataStruct.Value = key, value
	dataStruct.Type, dataStruct.Expire = Normal, 0
	return nil
}
func (b *batch) Delete(key []byte) error {
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
	dataStruct := b.db.recordPool.Get().(*baseDataStruct)
	b.appendPendingBaseData(key, dataStruct)
	dataStruct.Key, dataStruct.Value = key, nil
	dataStruct.Type, dataStruct.Expire = Deleted, 0
	return nil
}
func (b *batch) Commit() {
	//todo 先检查合法性
	// 然后把pendingBaseData里的数据用encodeBaseDataStruct加密成byte，然后用wal.PendingWrites写进去
	// 然后制造一个完成的数据同样加密并放进去
	// 然后就先告一段落
}
func (b *batch) checkPendingBaseData(key []byte) *baseDataStruct {
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

func (b *batch) appendPendingBaseData(key []byte, dataStruct *baseDataStruct) {
	//把baseDataStruct对象的指针先放到pendingBaseData中。
	b.pendingBaseData = append(b.pendingBaseData, dataStruct)
	if b.pendingBaseDataMap == nil {
		b.pendingBaseDataMap = make(map[uint64][]int)
	}
	hashKey := util.ByteHash(key)
	//然后更新map，len(b.pendingBaseData)-1是最后一个下标也就是刚才新加的指针的下标
	b.pendingBaseDataMap[hashKey] = append(b.pendingBaseDataMap[hashKey], len(b.pendingBaseData)-1)
}
