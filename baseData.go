package bitcaskdb

import (
	"encoding/binary"
	"os"

	"github.com/rosedblabs/wal"
	"github.com/valyala/bytebufferpool"
)

// baseStructType is the type of the log record.
type baseStructType = byte

const (
	// Normal baseData正常
	Normal baseStructType = iota
	// Deleted baseData删除
	Deleted
	// Finished baseData结束
	Finished
)

// keySize和valueSize用varInt，节省一点内存空间
type baseDataStruct struct {
	Key []byte
	//	keySize varInt
	Value []byte
	//	valueSize varInt
	Type    baseStructType
	BatchId uint64
	Expire  int64
}

// 最基本的数据写入加密 给commit用
func encodeBaseDataStruct(buf *bytebufferpool.ByteBuffer, header []byte, data *baseDataStruct) (res []byte) {
	// 这里buf是bytebufferpool的缓冲池，避免频繁make []byte
	// 注意：bytebufferpool.Get() 拿到的 buf.Bytes() 初始长度可能为0，不能直接下标写。
	if buf == nil {
		buf = bytebufferpool.Get()
		defer bytebufferpool.Put(buf)
	}
	buf.Reset()

	//0号位置是type
	header[0] = data.Type
	index := 1
	//1号是id
	index += binary.PutVarint(header[index:], int64(data.BatchId))
	index += binary.PutVarint(header[index:], data.Expire)
	//keySize和valueSize用varInt，节省一点内存空间
	index += binary.PutVarint(header[index:], int64(len(data.Key)))
	index += binary.PutVarint(header[index:], int64(len(data.Value)))

	_, _ = buf.Write(header[:index])
	_, _ = buf.Write(data.Key)
	_, _ = buf.Write(data.Value)
	return buf.Bytes()
}

// 最基本的数据解密 给get用，对应上面的加密
func decodeBaseDataStruct(data []byte) *baseDataStruct {
	//0号位置是type
	var index uint32 = 1

	//BatchId
	BatchId, i := binary.Varint(data[index:])
	if i <= 0 {
		return nil
	}
	index += uint32(i)

	//
	Expire, i := binary.Varint(data[index:])
	if i <= 0 {
		return nil
	}
	index += uint32(i)

	//
	keySize, i := binary.Varint(data[index:])
	if i <= 0 || keySize < 0 {
		return nil
	}
	index += uint32(i)

	//
	valueSize, i := binary.Varint(data[index:])
	if i <= 0 || valueSize < 0 {
		return nil
	}
	index += uint32(i)

	//
	if uint64(index)+uint64(keySize)+uint64(valueSize) > uint64(len(data)) {
		return nil
	}
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
		BatchId: uint64(BatchId),
		Expire:  Expire,
	}
}

// 加密hint文件
func encodeHintRecord(key []byte, pos *wal.ChunkPosition) []byte {
	// SegmentId BlockNumber ChunkOffset ChunkSize
	//    5          5           10          5      =    25
	// 这里是段数据,这个加密很少用，buf直接make就行
	buf := make([]byte, 25)
	idx := 0

	// SegmentId
	idx += binary.PutUvarint(buf[idx:], uint64(pos.SegmentId))
	// BlockNumber
	idx += binary.PutUvarint(buf[idx:], uint64(pos.BlockNumber))
	// ChunkOffset
	idx += binary.PutUvarint(buf[idx:], uint64(pos.ChunkOffset))
	// ChunkSize
	idx += binary.PutUvarint(buf[idx:], uint64(pos.ChunkSize))

	// key
	result := make([]byte, idx+len(key))
	copy(result, buf[:idx])
	copy(result[idx:], key)
	return result
}

func decodeHintRecord(buf []byte) ([]byte, *wal.ChunkPosition) {
	idx := 0
	// SegmentId
	segmentId, n := binary.Uvarint(buf[idx:])
	idx += n
	// BlockNumber
	blockNumber, n := binary.Uvarint(buf[idx:])
	idx += n
	// ChunkOffset
	chunkOffset, n := binary.Uvarint(buf[idx:])
	idx += n
	// ChunkSize
	chunkSize, n := binary.Uvarint(buf[idx:])
	idx += n
	// Key
	key := buf[idx:]
	return key, &wal.ChunkPosition{
		SegmentId:   wal.SegmentID(segmentId),
		BlockNumber: uint32(blockNumber),
		ChunkOffset: int64(chunkOffset),
		ChunkSize:   uint32(chunkSize),
	}
}

// merge数据加密，这个本质只存了1个merge时的最后一个seg
func encodeMergeFinish(segmentId wal.SegmentID) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, segmentId)
	return buf
}
func getMergeFinSegmentId(mergePath string) (wal.SegmentID, error) {
	// 检查在不在
	mergeFinFile, err := os.Open(wal.SegmentFileName(mergePath, mergeFinNameSuffix, 1))
	if err != nil {
		//不在直接返回
		return 0, nil
	}
	defer func() {
		_ = mergeFinFile.Close()
	}()

	// encodeMergeFinish 加密的时候的SegmentID只有4字节，然后前面7字节是wal的头
	mergeFinBuf := make([]byte, 4)
	if _, err := mergeFinFile.ReadAt(mergeFinBuf, 7); err != nil {
		return 0, err
	}
	mergeFinSegmentId := binary.LittleEndian.Uint32(mergeFinBuf)
	return mergeFinSegmentId, nil
}
