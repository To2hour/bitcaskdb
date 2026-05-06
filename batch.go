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
	//用来存pendingBaseData的map，key是uint，因为这里直接调用hash算，value是切片是可能hash碰撞
	pendingBaseDataMap map[uint64][]int
	mu                 sync.RWMutex
	committed          bool
}

func (b *batch) put(key, value []byte) error {
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
		data.Type = LogRecordNormal
		data.Expire = 0
		return nil
	}
	//todo 真正写入的逻辑
	return nil
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
