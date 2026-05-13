package bitcaskdb

import (
	"bitcaskdb/index"
	"fmt"
	"sync"
	"testing"

	"github.com/rosedblabs/wal"
)

// newTestBatchDB 创建只初始化了 WAL 和 index 的轻量 DB，用于白盒测试 Batch 内部逻辑。
func newTestBatchDB(t *testing.T) *DB {
	t.Helper()
	dir := t.TempDir()
	opt := wal.DefaultOptions
	opt.DirPath = dir
	dataFiles, err := wal.Open(opt)
	if err != nil {
		t.Fatalf("open wal failed: %v", err)
	}
	t.Cleanup(func() { _ = dataFiles.Close() })

	return &DB{
		dataFiles: dataFiles,
		index:     index.NewIndexer(),
		mu:        sync.RWMutex{},
		baseDataStructPool: sync.Pool{New: func() any {
			return &baseDataStruct{}
		}},
		encodeHeader: make([]byte, maxBaseDataHeaderSize),
	}
}

// commitBatch 先获取 DB 写锁再 commit（batch 设计要求）。
func commitBatch(t *testing.T, b *Batch) {
	t.Helper()
	b.Lock()
	if err := b.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
}

// ======================== Batch 基础功能 ========================

func TestBatch_PutCommitVerifyWAL(t *testing.T) {
	db := newTestBatchDB(t)
	b := db.NewBatch()
	_ = b.Put([]byte("key"), []byte("value"))
	_ = b.Put([]byte("key1"), []byte("value1"))
	_ = b.Put([]byte("key2"), []byte("value2"))
	_ = b.Delete([]byte("key2"))
	commitBatch(t, b)

	// key / key1 应在 index 中，且 WAL 里可以解码出来
	for _, k := range [][]byte{[]byte("key"), []byte("key1")} {
		pos := db.index.Get(k)
		if pos == nil {
			t.Fatalf("expected key %q in index after commit", k)
		}
		val, err := db.dataFiles.Read(pos)
		if err != nil {
			t.Fatalf("Read WAL failed for key %q: %v", k, err)
		}
		rec := decodeBaseDataStruct(val)
		if rec == nil {
			t.Fatalf("decode failed for key %q", k)
		}
		if rec.Type != Normal {
			t.Fatalf("key %q: expected Normal type, got %d", k, rec.Type)
		}
	}

	// key2 已删除，不应在 index 中
	if pos := db.index.Get([]byte("key2")); pos != nil {
		t.Fatal("key2 should be deleted from index")
	}
}

func TestBatch_FinishedRecordInWAL(t *testing.T) {
	db := newTestBatchDB(t)
	b := db.NewBatch()
	_ = b.Put([]byte("a"), []byte("1"))
	commitBatch(t, b)

	// WAL 中必须有 Finished 标记，保证崩溃恢复的原子性
	reader := db.dataFiles.NewReader()
	foundFinished := false
	for {
		chunk, _, err := reader.Next()
		if err != nil {
			break
		}
		rec := decodeBaseDataStruct(chunk)
		if rec != nil && rec.Type == Finished {
			foundFinished = true
			break
		}
	}
	if !foundFinished {
		t.Fatal("expected Finished record in WAL after commit")
	}
}

func TestBatch_GetBeforeCommitReadsFromPending(t *testing.T) {
	db := newTestBatchDB(t)
	b := db.NewBatch()
	_ = b.Put([]byte("staged"), []byte("pending-value"))

	// 不需要 DB 锁，Get 只用 batch 自己的 mu
	val, err := b.Get([]byte("staged"))
	if err != nil {
		t.Fatalf("Get from pending failed: %v", err)
	}
	if string(val) != "pending-value" {
		t.Fatalf("expected 'pending-value', got %q", val)
	}
	commitBatch(t, b)
}

func TestBatch_GetMissingKeyReturnsError(t *testing.T) {
	db := newTestBatchDB(t)
	b := db.NewBatch()
	_, err := b.Get([]byte("ghost"))
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got: %v", err)
	}
}

func TestBatch_DeleteMarksPendingAsDeleted(t *testing.T) {
	db := newTestBatchDB(t)
	b := db.NewBatch()
	_ = b.Put([]byte("x"), []byte("val"))
	_ = b.Delete([]byte("x"))

	// Deleted 状态 value 应为 nil
	val, _ := b.Get([]byte("x"))
	if val != nil {
		t.Fatalf("expected nil value for deleted key in pending, got %q", val)
	}
	commitBatch(t, b)
}

func TestBatch_OverwriteKeyUpdatesInPlace(t *testing.T) {
	db := newTestBatchDB(t)
	b := db.NewBatch()
	_ = b.Put([]byte("dup"), []byte("first"))
	_ = b.Put([]byte("dup"), []byte("second"))

	val, err := b.Get([]byte("dup"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(val) != "second" {
		t.Fatalf("expected 'second', got %q", val)
	}
	commitBatch(t, b)
}

func TestBatch_MultipleBatchesSequential(t *testing.T) {
	db := newTestBatchDB(t)
	for i := 0; i < 5; i++ {
		b := db.NewBatch()
		for j := 0; j < 10; j++ {
			_ = b.Put([]byte(fmt.Sprintf("b%d-k%d", i, j)), []byte(fmt.Sprintf("v%d-%d", i, j)))
		}
		commitBatch(t, b)
	}

	for i := 0; i < 5; i++ {
		for j := 0; j < 10; j++ {
			key := fmt.Sprintf("b%d-k%d", i, j)
			if pos := db.index.Get([]byte(key)); pos == nil {
				t.Fatalf("key %q not found in index", key)
			}
		}
	}
}

// ======================== 错误路径 ========================

func TestBatch_PutAfterCommitFails(t *testing.T) {
	db := newTestBatchDB(t)
	b := db.NewBatch()
	_ = b.Put([]byte("k"), []byte("v"))
	commitBatch(t, b)

	if err := b.Put([]byte("k2"), []byte("v2")); err != ErrBatchCommitted {
		t.Fatalf("expected ErrBatchCommitted, got: %v", err)
	}
}

func TestBatch_DeleteAfterCommitFails(t *testing.T) {
	db := newTestBatchDB(t)
	b := db.NewBatch()
	_ = b.Put([]byte("k"), []byte("v"))
	commitBatch(t, b)

	if err := b.Delete([]byte("k")); err != ErrBatchCommitted {
		t.Fatalf("expected ErrBatchCommitted, got: %v", err)
	}
}

func TestBatch_CommitReadOnlyFails(t *testing.T) {
	db := newTestBatchDB(t)
	b := &Batch{db: db, readOnly: true}
	b.Lock() // readOnly → RLock；Commit 的 defer b.Unlock() 会配对释放
	if err := b.Commit(); err != ErrReadOnlyBatch {
		t.Fatalf("expected ErrReadOnlyBatch, got: %v", err)
	}
}

func TestBatch_RollbackReadOnlyFails(t *testing.T) {
	db := newTestBatchDB(t)
	b := &Batch{db: db, readOnly: true}
	b.Lock() // readOnly → RLock；Rollback 的 defer b.Unlock() 会配对释放
	if err := b.Rollback(); err != ErrReadOnlyBatch {
		t.Fatalf("expected ErrReadOnlyBatch, got: %v", err)
	}
}

func TestBatch_RollbackClearsPending(t *testing.T) {
	db := newTestBatchDB(t)
	b := db.NewBatch()
	_ = b.Put([]byte("r1"), []byte("v1"))
	_ = b.Put([]byte("r2"), []byte("v2"))

	// Rollback 需要持有 DB 写锁（对称于 Commit）
	b.Lock()
	if err := b.Rollback(); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	if pos := db.index.Get([]byte("r1")); pos != nil {
		t.Fatal("r1 should not be in index after rollback")
	}
	if pos := db.index.Get([]byte("r2")); pos != nil {
		t.Fatal("r2 should not be in index after rollback")
	}
}
