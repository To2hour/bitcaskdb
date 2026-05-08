package bitcaskdb

import (
	"bitcaskdb/index"
	"fmt"
	"sync"
	"testing"

	"github.com/rosedblabs/wal"
)

func TestBatch(t *testing.T) {
	t.Log("提示：请用 `go test -v ./...` 查看日志输出")
	//强行new一个db。把目前用到的dataFiles给open即可
	// 然后绑定上。我测试下put，delete，commit。没啥问题了我就把get写了
	dir := t.TempDir()
	fmt.Println(dir)
	opt := wal.DefaultOptions
	opt.DirPath = dir

	dataFiles, err := wal.Open(opt)
	if err != nil {
		t.Fatalf("open wal failed: %v", err)
	}
	t.Cleanup(func() { _ = dataFiles.Close() })

	db := &DB{
		dataFiles: dataFiles,
		index:     index.NewIndexer(),
		mu:        sync.RWMutex{},
		baseDataStructPool: sync.Pool{New: func() any {
			return &baseDataStruct{}
		}},
	}

	b := db.NewBatch()
	if err := b.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("put failed: %v", err)
	}
	_ = b.Put([]byte("key1"), []byte("value1"))
	_ = b.Put([]byte("key2"), []byte("value2"))
	_ = b.Put([]byte("key3"), []byte("value3"))
	_ = b.Delete([]byte("key3"))
	_ = b.Delete([]byte("key2"))
	fmt.Print("key1 ==  ")
	val, _ := b.Get([]byte("key1"))
	fmt.Println(string(val))
	if err := b.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	t.Log("commit OK")
	fmt.Print("key1 ==  ")
	val1, _ := b.Get([]byte("key1"))
	fmt.Println(string(val1))
	// key / key1 应该存在
	for _, k := range [][]byte{[]byte("key"), []byte("key1")} {
		pos := db.index.Get(k)
		if pos == nil {
			t.Fatalf("expected key %q in index", string(k))
		}
		t.Logf("index hit key=%q pos=%v", string(k), pos)
		val, err := db.dataFiles.Read(pos)
		if err != nil {
			t.Fatalf("read wal failed: %v", err)
		}
		rec := decodeBaseDataStruct(val)
		if rec == nil {
			t.Fatalf("decode failed for key %q", string(k))
		}
		t.Logf("wal decode key=%q type=%d batchId=%d expire=%d valueLen=%d", string(rec.Key), rec.Type, rec.BatchId, rec.Expire, len(rec.Value))
		if string(rec.Key) != string(k) {
			t.Fatalf("key mismatch: got %q want %q", string(rec.Key), string(k))
		}
		if rec.Type != Normal {
			t.Fatalf("type mismatch: got %d want %d", rec.Type, Normal)
		}
	}

	// key2 / key3 应该被删除
	for _, k := range [][]byte{[]byte("key2"), []byte("key3")} {
		pos := db.index.Get(k)
		if pos != nil {
			t.Fatalf("expected key %q deleted, but index has pos %v", string(k), pos)
		}
		t.Logf("index delete OK key=%q", string(k))
	}

	// WAL 里应当至少有 1 条 Finished 记录
	reader := db.dataFiles.NewReader()
	foundFinished := false
	for {
		chunk, _, err := reader.Next()
		if err != nil {
			break
		}
		rec := decodeBaseDataStruct(chunk)
		if rec != nil {
			t.Logf("scan wal type=%d keyLen=%d valueLen=%d batchId=%d", rec.Type, len(rec.Key), len(rec.Value), rec.BatchId)
			if rec.Type == Finished {
				foundFinished = true
				break
			}
		}
	}
	if !foundFinished {
		t.Fatalf("expected Finished record in wal")
	}
	t.Log("found Finished record")
}
