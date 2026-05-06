package bitcaskdb

import (
	"encoding/binary"
)

// LogRecordType is the type of the log record.
type LogRecordType = byte

const (
	// LogRecordNormal is the normal log record type.
	LogRecordNormal LogRecordType = iota
	// LogRecordDeleted is the deleted log record type.
	LogRecordDeleted
	// LogRecordBatchFinished is the batch finished log record type.
	LogRecordBatchFinished
)

// keySize和valueSize用varInt，节省一点内存空间
type baseDataStruct struct {
	Key []byte
	//	keySize varInt
	Value []byte
	//	valueSize varInt
	Type    LogRecordType
	BatchId uint64
	Expire  int64
}

// 最基本的数据写入加密 给commit用
func encodeBaseDataStruct(buf []byte, data baseDataStruct) (res []byte) {
	//0号位置是type
	buf[0] = data.Type
	index := 1
	//1号是id
	index += binary.PutUvarint(buf[index:], data.BatchId)
	index += binary.PutUvarint(buf[index:], uint64(data.Expire))
	//keySize
	index += binary.PutVarint(buf[index:], int64(len(data.Key)))
	index += binary.PutVarint(buf[index:], int64(len(data.Value)))
	_ = append(res, buf...)
	_ = append(res, data.Key...)
	_ = append(res, data.Value...)
	return
}

// 最基本的数据解密 给get用，对应上面的加密
func decodeBaseDataStruct(data []byte) *baseDataStruct {
	//0号位置是type
	var index uint32 = 1

	//BatchId
	BatchId, i := binary.Uvarint(data[index:])
	index += uint32(i)

	//
	Expire, i := binary.Uvarint(data[index:])
	index += uint32(i)

	//
	keySize, i := binary.Uvarint(data[index:])
	index += uint32(i)

	//
	valueSize, i := binary.Uvarint(data[index:])
	index += uint32(i)

	//
	key := make([]byte, keySize)
	copy(key, data[index:index+uint32(keySize)])
	index += uint32(keySize)

	//
	value := make([]byte, valueSize)
	copy(value, data[index:index+uint32(valueSize)])
	index += uint32(valueSize)
	return &baseDataStruct{
		Key:     key,
		Value:   value,
		Type:    data[0],
		BatchId: BatchId,
		Expire:  int64(Expire),
	}
}
