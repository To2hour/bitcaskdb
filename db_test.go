package bitcaskdb

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

// newTestDB 在临时目录打开一个 DB，测试结束后自动关闭并清理目录。
func newTestDB(t *testing.T) *DB {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(&Options{DirPath: dir, SegmentSize: 64 * MB})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// newTestDBInDir 在指定目录打开 DB，适合需要多次 reopen 的测试。
func newTestDBInDir(t *testing.T, dir string) *DB {
	t.Helper()
	db, err := Open(&Options{DirPath: dir, SegmentSize: 64 * MB})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	return db
}

// ======================== Open ========================

func TestOpen_Valid(t *testing.T) {
	db := newTestDB(t)
	if db == nil {
		t.Fatal("expected non-nil DB")
	}
}

func TestOpen_EmptyDirPath(t *testing.T) {
	_, err := Open(&Options{DirPath: "", SegmentSize: GB})
	if err == nil {
		t.Fatal("expected error for empty DirPath")
	}
}

func TestOpen_ZeroSegmentSize(t *testing.T) {
	_, err := Open(&Options{DirPath: t.TempDir(), SegmentSize: 0})
	if err == nil {
		t.Fatal("expected error for zero SegmentSize")
	}
}

func TestOpen_InvalidCronExpr(t *testing.T) {
	_, err := Open(&Options{
		DirPath:           t.TempDir(),
		SegmentSize:       GB,
		AutoMergeCronExpr: "not-a-cron-expr",
	})
	if err == nil {
		t.Fatal("expected error for invalid cron expression")
	}
}

func TestOpen_ValidCronExpr(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(&Options{
		DirPath:           dir,
		SegmentSize:       GB,
		AutoMergeCronExpr: "*/5 * * * *",
	})
	if err != nil {
		t.Fatalf("unexpected error for valid cron: %v", err)
	}
	_ = db.Close()
}

func TestOpen_EmptyCronExpr(t *testing.T) {
	// 空字符串代表不启用自动合并，不应报错
	dir := t.TempDir()
	db, err := Open(&Options{DirPath: dir, SegmentSize: GB, AutoMergeCronExpr: ""})
	if err != nil {
		t.Fatalf("empty cron should not error: %v", err)
	}
	_ = db.Close()
}

func TestOpen_AutoCreateDirectory(t *testing.T) {
	base := t.TempDir()
	nested := base + "/sub/dir"
	db, err := Open(&Options{DirPath: nested, SegmentSize: GB})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	_ = db.Close()
	if _, err := os.Stat(nested); os.IsNotExist(err) {
		t.Fatal("expected nested directory to be created")
	}
}

func TestOpen_DirectoryAlreadyInUse(t *testing.T) {
	dir := t.TempDir()
	db1, err := Open(&Options{DirPath: dir, SegmentSize: GB})
	if err != nil {
		t.Fatalf("first Open failed: %v", err)
	}
	defer db1.Close()

	_, err = Open(&Options{DirPath: dir, SegmentSize: GB})
	if err != ErrDatabaseIsUsing {
		t.Fatalf("expected ErrDatabaseIsUsing, got: %v", err)
	}
}

// ======================== Put / Get / Delete ========================

func TestPut_BasicGetReturnsValue(t *testing.T) {
	db := newTestDB(t)
	if err := db.Put([]byte("hello"), []byte("world")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	val, err := db.Get([]byte("hello"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(val) != "world" {
		t.Fatalf("expected 'world', got %q", val)
	}
}

func TestPut_OverwriteReturnsLatestValue(t *testing.T) {
	db := newTestDB(t)
	_ = db.Put([]byte("k"), []byte("v1"))
	_ = db.Put([]byte("k"), []byte("v2"))
	_ = db.Put([]byte("k"), []byte("v3"))
	val, err := db.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(val) != "v3" {
		t.Fatalf("expected 'v3', got %q", val)
	}
}

func TestGet_KeyNotFound(t *testing.T) {
	db := newTestDB(t)
	_, err := db.Get([]byte("nonexistent"))
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound, got: %v", err)
	}
}

func TestPut_EmptyValue(t *testing.T) {
	db := newTestDB(t)
	if err := db.Put([]byte("emptyval"), []byte{}); err != nil {
		t.Fatalf("Put empty value failed: %v", err)
	}
	val, err := db.Get([]byte("emptyval"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if len(val) != 0 {
		t.Fatalf("expected empty value, got %q", val)
	}
}

func TestPut_LargeValue(t *testing.T) {
	db := newTestDB(t)
	large := make([]byte, 1*MB)
	for i := range large {
		large[i] = byte(i % 251)
	}
	if err := db.Put([]byte("bigkey"), large); err != nil {
		t.Fatalf("Put large value failed: %v", err)
	}
	got, err := db.Get([]byte("bigkey"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if len(got) != len(large) {
		t.Fatalf("value length mismatch: got %d, want %d", len(got), len(large))
	}
	for i := range large {
		if got[i] != large[i] {
			t.Fatalf("value mismatch at index %d", i)
		}
	}
}

func TestDelete_RemovesKey(t *testing.T) {
	db := newTestDB(t)
	_ = db.Put([]byte("del"), []byte("me"))
	if err := db.Delete([]byte("del")); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	_, err := db.Get([]byte("del"))
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound after delete, got: %v", err)
	}
}

func TestDelete_NonExistentKeyNoError(t *testing.T) {
	db := newTestDB(t)
	if err := db.Delete([]byte("ghost")); err != nil {
		t.Fatalf("Delete of non-existent key should not fail: %v", err)
	}
}

func TestPut_MultipleKeys(t *testing.T) {
	db := newTestDB(t)
	const n = 1000
	for i := 0; i < n; i++ {
		key := []byte("key" + strconv.Itoa(i))
		val := []byte("val" + strconv.Itoa(i))
		if err := db.Put(key, val); err != nil {
			t.Fatalf("Put key%d failed: %v", i, err)
		}
	}
	for i := 0; i < n; i++ {
		key := []byte("key" + strconv.Itoa(i))
		want := "val" + strconv.Itoa(i)
		got, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get key%d failed: %v", i, err)
		}
		if string(got) != want {
			t.Fatalf("key%d: expected %q, got %q", i, want, got)
		}
	}
}

// ======================== 持久化（关闭后重新打开）========================

func TestPersistence_PutSurvivesReopen(t *testing.T) {
	dir := t.TempDir()

	db := newTestDBInDir(t, dir)
	_ = db.Put([]byte("engine"), []byte("bitcaskdb"))
	_ = db.Close()

	db2 := newTestDBInDir(t, dir)
	defer db2.Close()
	val, err := db2.Get([]byte("engine"))
	if err != nil {
		t.Fatalf("Get after reopen failed: %v", err)
	}
	if string(val) != "bitcaskdb" {
		t.Fatalf("expected 'bitcaskdb', got %q", val)
	}
}

func TestPersistence_DeleteSurvivesReopen(t *testing.T) {
	dir := t.TempDir()

	db := newTestDBInDir(t, dir)
	_ = db.Put([]byte("temp"), []byte("data"))
	_ = db.Delete([]byte("temp"))
	_ = db.Close()

	db2 := newTestDBInDir(t, dir)
	defer db2.Close()
	_, err := db2.Get([]byte("temp"))
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound after delete+reopen, got: %v", err)
	}
}

func TestPersistence_OverwriteSurvivesReopen(t *testing.T) {
	dir := t.TempDir()

	db := newTestDBInDir(t, dir)
	_ = db.Put([]byte("k"), []byte("old"))
	_ = db.Put([]byte("k"), []byte("new"))
	_ = db.Close()

	db2 := newTestDBInDir(t, dir)
	defer db2.Close()
	val, err := db2.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(val) != "new" {
		t.Fatalf("expected 'new', got %q", val)
	}
}

func TestPersistence_BulkDataSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	const n = 500

	db := newTestDBInDir(t, dir)
	for i := 0; i < n; i++ {
		_ = db.Put([]byte(fmt.Sprintf("k%04d", i)), []byte(fmt.Sprintf("v%d", i)))
	}
	_ = db.Close()

	db2 := newTestDBInDir(t, dir)
	defer db2.Close()
	if db2.index.Size() != n {
		t.Fatalf("index size mismatch: got %d, want %d", db2.index.Size(), n)
	}
	for i := 0; i < n; i++ {
		want := fmt.Sprintf("v%d", i)
		got, err := db2.Get([]byte(fmt.Sprintf("k%04d", i)))
		if err != nil {
			t.Fatalf("Get k%04d failed: %v", i, err)
		}
		if string(got) != want {
			t.Fatalf("k%04d: expected %q, got %q", i, want, got)
		}
	}
}

// ======================== Merge ========================

func TestMerge_DataIntactAfterMerge(t *testing.T) {
	db := newTestDB(t)
	const n = 500
	for i := 0; i < n; i++ {
		_ = db.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i)))
	}
	// 覆盖前半段，产生大量无效数据
	for i := 0; i < n/2; i++ {
		_ = db.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("updated%d", i)))
	}
	// 删除后 1/4
	for i := n * 3 / 4; i < n; i++ {
		_ = db.Delete([]byte(fmt.Sprintf("k%d", i)))
	}

	if err := db.Merge(); err != nil {
		t.Fatalf("Merge failed: %v", err)
	}

	// 验证覆盖段返回新值
	for i := 0; i < n/2; i++ {
		want := fmt.Sprintf("updated%d", i)
		got, err := db.Get([]byte(fmt.Sprintf("k%d", i)))
		if err != nil {
			t.Fatalf("Get k%d after Merge failed: %v", i, err)
		}
		if string(got) != want {
			t.Fatalf("k%d: expected %q, got %q", i, want, got)
		}
	}
	// 验证未覆盖段返回原值
	for i := n / 2; i < n*3/4; i++ {
		want := fmt.Sprintf("v%d", i)
		got, err := db.Get([]byte(fmt.Sprintf("k%d", i)))
		if err != nil {
			t.Fatalf("Get k%d after Merge failed: %v", i, err)
		}
		if string(got) != want {
			t.Fatalf("k%d: expected %q, got %q", i, want, got)
		}
	}
	// 验证删除段已不存在
	for i := n * 3 / 4; i < n; i++ {
		_, err := db.Get([]byte(fmt.Sprintf("k%d", i)))
		if err != ErrKeyNotFound {
			t.Fatalf("k%d should not exist after Merge+delete, got: %v", i, err)
		}
	}
}

func TestMerge_ReopenAfterMerge(t *testing.T) {
	dir := t.TempDir()
	const n = 300

	db := newTestDBInDir(t, dir)
	for i := 0; i < n; i++ {
		_ = db.Put([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i)))
	}
	// 删除前半段
	for i := 0; i < n/2; i++ {
		_ = db.Delete([]byte(fmt.Sprintf("key%d", i)))
	}
	if err := db.Merge(); err != nil {
		t.Fatalf("Merge failed: %v", err)
	}
	_ = db.Close()

	// 重启，验证数据完整性
	db2 := newTestDBInDir(t, dir)
	defer db2.Close()

	// 前半段已删，应找不到
	for i := 0; i < n/2; i++ {
		_, err := db2.Get([]byte(fmt.Sprintf("key%d", i)))
		if err != ErrKeyNotFound {
			t.Fatalf("key%d should be deleted after Merge+reopen, got: %v", i, err)
		}
	}
	// 后半段存在
	for i := n / 2; i < n; i++ {
		want := fmt.Sprintf("val%d", i)
		got, err := db2.Get([]byte(fmt.Sprintf("key%d", i)))
		if err != nil {
			t.Fatalf("Get key%d after Merge+reopen failed: %v", i, err)
		}
		if string(got) != want {
			t.Fatalf("key%d: expected %q, got %q", i, want, got)
		}
	}
}

func TestMerge_ConcurrentWritesNotLost(t *testing.T) {
	db := newTestDB(t)

	// 先写入一批旧数据
	for i := 0; i < 200; i++ {
		_ = db.Put([]byte(fmt.Sprintf("old%d", i)), []byte("oldval"))
	}

	// Merge 和并发写同时进行
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = db.Merge()
	}()
	for i := 0; i < 100; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = db.Put([]byte(fmt.Sprintf("new%d", i)), []byte(fmt.Sprintf("newval%d", i)))
		}()
	}
	wg.Wait()

	// 并发写入的新数据不应丢失
	for i := 0; i < 100; i++ {
		val, err := db.Get([]byte(fmt.Sprintf("new%d", i)))
		if err != nil {
			t.Fatalf("Get new%d after concurrent Merge failed: %v", i, err)
		}
		if string(val) != fmt.Sprintf("newval%d", i) {
			t.Fatalf("new%d: expected 'newval%d', got %q", i, i, val)
		}
	}
}

func TestMerge_RunningTwiceReturnsError(t *testing.T) {
	db := newTestDB(t)
	for i := 0; i < 100; i++ {
		_ = db.Put([]byte(fmt.Sprintf("k%d", i)), []byte("v"))
	}
	// 直接用原子操作模拟 Merge 正在运行的状态
	atomic.StoreUint32(&db.mergeRunning, 1)
	defer atomic.StoreUint32(&db.mergeRunning, 0)

	err := db.DoMerge()
	if err != ErrMergeRunning {
		t.Fatalf("expected ErrMergeRunning, got: %v", err)
	}
}

// ======================== 并发安全 ========================

func TestConcurrent_Writes(t *testing.T) {
	db := newTestDB(t)
	const goroutines = 50
	const perGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				key := fmt.Sprintf("g%d-k%d", g, i)
				val := fmt.Sprintf("v%d-%d", g, i)
				if err := db.Put([]byte(key), []byte(val)); err != nil {
					t.Errorf("Put %s failed: %v", key, err)
				}
			}
		}()
	}
	wg.Wait()

	// 抽样验证
	for g := 0; g < goroutines; g++ {
		key := fmt.Sprintf("g%d-k0", g)
		if _, err := db.Get([]byte(key)); err != nil {
			t.Fatalf("Get %s failed: %v", key, err)
		}
	}
}

func TestConcurrent_MixedReadWrite(t *testing.T) {
	db := newTestDB(t)
	// 先写入种子数据
	for i := 0; i < 200; i++ {
		_ = db.Put([]byte(fmt.Sprintf("seed%d", i)), []byte("seedval"))
	}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		i := i
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = db.Put([]byte(fmt.Sprintf("write%d", i)), []byte("v"))
		}()
		go func() {
			defer wg.Done()
			_, _ = db.Get([]byte(fmt.Sprintf("seed%d", i%200)))
		}()
	}
	wg.Wait()
}

func TestConcurrent_PutGetDelete(t *testing.T) {
	db := newTestDB(t)
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := []byte(fmt.Sprintf("ck%d", i))
			_ = db.Put(key, []byte("v"))
			_, _ = db.Get(key)
			_ = db.Delete(key)
		}()
	}
	wg.Wait()
}

// ======================== Batch ========================

func TestBatch_CommitMultipleOps(t *testing.T) {
	db := newTestDB(t)
	b := db.NewBatch()
	b.Lock()
	_ = b.Put([]byte("a"), []byte("1"))
	_ = b.Put([]byte("b"), []byte("2"))
	_ = b.Put([]byte("c"), []byte("3"))
	if err := b.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	expected := map[string]string{"a": "1", "b": "2", "c": "3"}
	for k, want := range expected {
		got, err := db.Get([]byte(k))
		if err != nil {
			t.Fatalf("Get %q failed: %v", k, err)
		}
		if string(got) != want {
			t.Fatalf("%q: expected %q, got %q", k, want, got)
		}
	}
}

func TestBatch_PutThenDeleteInSameBatch(t *testing.T) {
	db := newTestDB(t)
	b := db.NewBatch()
	b.Lock()
	_ = b.Put([]byte("x"), []byte("val"))
	_ = b.Delete([]byte("x"))
	_ = b.Commit()

	_, err := db.Get([]byte("x"))
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got: %v", err)
	}
}

func TestBatch_GetFromPendingBeforeCommit(t *testing.T) {
	db := newTestDB(t)
	b := db.NewBatch()
	b.Lock()
	_ = b.Put([]byte("pending"), []byte("fromPending"))

	// Commit 前从 pending 读取
	val, err := b.Get([]byte("pending"))
	if err != nil {
		t.Fatalf("Get from pending failed: %v", err)
	}
	if string(val) != "fromPending" {
		t.Fatalf("expected 'fromPending', got %q", val)
	}
	_ = b.Commit()
}

func TestBatch_PutAfterCommitReturnsError(t *testing.T) {
	db := newTestDB(t)
	b := db.NewBatch()
	b.Lock()
	_ = b.Put([]byte("k"), []byte("v"))
	_ = b.Commit()

	err := b.Put([]byte("k2"), []byte("v2"))
	if err != ErrBatchCommitted {
		t.Fatalf("expected ErrBatchCommitted, got: %v", err)
	}
}

func TestBatch_DeleteAfterCommitReturnsError(t *testing.T) {
	db := newTestDB(t)
	b := db.NewBatch()
	b.Lock()
	_ = b.Put([]byte("k"), []byte("v"))
	_ = b.Commit()

	err := b.Delete([]byte("k"))
	if err != ErrBatchCommitted {
		t.Fatalf("expected ErrBatchCommitted, got: %v", err)
	}
}

func TestBatch_Rollback(t *testing.T) {
	db := newTestDB(t)
	b := db.NewBatch()
	b.Lock()
	_ = b.Put([]byte("rollme"), []byte("nope"))
	if err := b.Rollback(); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	_, err := db.Get([]byte("rollme"))
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound after rollback, got: %v", err)
	}
}

func TestBatch_ReadOnlyCommitReturnsError(t *testing.T) {
	db := newTestDB(t)
	_ = db.Put([]byte("ro"), []byte("val"))

	b := db.NewBatch()
	b.init(true, db) // 只读
	b.Lock()         // RLock；Commit 的 defer b.Unlock() 会配对释放，不能再 defer Unlock

	err := b.Commit()
	if err != ErrReadOnlyBatch {
		t.Fatalf("expected ErrReadOnlyBatch, got: %v", err)
	}
}

func TestBatch_ReadOnlyRollbackReturnsError(t *testing.T) {
	db := newTestDB(t)

	b := db.NewBatch()
	b.init(true, db)
	b.Lock() // RLock；Rollback 的 defer b.Unlock() 会配对释放

	err := b.Rollback()
	if err != ErrReadOnlyBatch {
		t.Fatalf("expected ErrReadOnlyBatch, got: %v", err)
	}
}

func TestBatch_OverwriteKeyInSameBatch(t *testing.T) {
	db := newTestDB(t)
	b := db.NewBatch()
	b.Lock()
	_ = b.Put([]byte("dup"), []byte("first"))
	_ = b.Put([]byte("dup"), []byte("second"))
	_ = b.Commit()

	val, err := db.Get([]byte("dup"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(val) != "second" {
		t.Fatalf("expected 'second', got %q", val)
	}
}

// ======================== Close ========================

func TestClose_ReleasesFileLock(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(&Options{DirPath: dir, SegmentSize: GB})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	// 关闭后同目录可以再次被打开
	db2, err := Open(&Options{DirPath: dir, SegmentSize: GB})
	if err != nil {
		t.Fatalf("reopen after close failed: %v", err)
	}
	_ = db2.Close()
}

func TestClose_WithCronScheduler(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(&Options{
		DirPath:           dir,
		SegmentSize:       GB,
		AutoMergeCronExpr: "*/1 * * * *",
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close with cron scheduler failed: %v", err)
	}
}
