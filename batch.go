package bitcaskdb

import "sync"

// 作用：弄一个数组baseData，然后commit的时候一下写完。然后实现get put commit delete
type batch struct {
	//待写入的数据
	pendingBaseData []*baseDataStruct
	//用来存pendingBaseData的map，key是uint，因为这里直接调用hash算，value是切片是可能hash碰撞
	pendingBaseDataMap map[uint64][]int
	mu                 sync.RWMutex
	committed          bool
}

func (b *batch) put(key, value []byte) {

}
