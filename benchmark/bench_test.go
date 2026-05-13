// Package benchmark 提供 bitcaskdb 的性能基准测试。
//
// 运行方式（在项目根目录执行）：
//
//	go test -bench="Benchmark" -benchmem -benchtime=3s -run="^$" ./benchmark/
//
// 实测结果（Intel i7-14700HX，Windows，128B value）：
//
//	BenchmarkPut-28              320425     7214 ns/op    567 B/op   11 allocs/op
//	BenchmarkPut_Overwrite-28    327026     7027 ns/op    490 B/op    9 allocs/op
//	BenchmarkGet-28              697806     3673 ns/op    480 B/op    9 allocs/op
//	BenchmarkBatchPut100-28       20547   107869 ns/op  121647 B/op  855 allocs/op  (~926K key/s)
//	BenchmarkDelete-28           299775     7838 ns/op    512 B/op   11 allocs/op
//	BenchmarkPutParallel-28      247880     9546 ns/op    588 B/op   11 allocs/op
//	BenchmarkGetParallel-28      399093     5610 ns/op    480 B/op    9 allocs/op
//	BenchmarkMixedReadWrite-28   323163     7453 ns/op    512 B/op    9 allocs/op
//	BenchmarkReopen-28               49  55066018 ns/op               (10000 keys, ~55ms 冷启动)
package benchmark_test

import (
	"bitcaskdb"
	"fmt"
	"os"
	"strconv"
	"testing"
)

// benchValue 是每次写入使用的固定 value，128 字节模拟真实业务场景。
var benchValue = make([]byte, 128)

func init() {
	for i := range benchValue {
		benchValue[i] = byte(i % 251)
	}
}

// openBenchDB 在临时目录打开一个 DB，返回 DB 实例和清理函数。
func openBenchDB(b *testing.B) (*bitcaskdb.DB, func()) {
	b.Helper()
	dir, err := os.MkdirTemp("", "bitcaskdb-bench-*")
	if err != nil {
		b.Fatalf("MkdirTemp failed: %v", err)
	}
	db, err := bitcaskdb.Open(&bitcaskdb.Options{
		DirPath:     dir,
		SegmentSize: bitcaskdb.GB,
	})
	if err != nil {
		_ = os.RemoveAll(dir)
		b.Fatalf("Open failed: %v", err)
	}
	return db, func() {
		_ = db.Close()
		_ = os.RemoveAll(dir)
	}
}

// ======================== 顺序写 ========================

// BenchmarkPut 测试单条顺序写入吞吐，key 不重复（无覆盖）。
// 157640	      6786 ns/op
func BenchmarkPut(b *testing.B) {
	db, cleanup := openBenchDB(b)
	defer cleanup()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := strconv.AppendInt([]byte("key-"), int64(i), 10)
		if err := db.Put(key, benchValue); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPut_Overwrite 测试反复覆盖同一个 key 的写入吞吐。
func BenchmarkPut_Overwrite(b *testing.B) {
	db, cleanup := openBenchDB(b)
	defer cleanup()
	key := []byte("overwrite-key")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Put(key, benchValue); err != nil {
			b.Fatal(err)
		}
	}
}

// ======================== 顺序读 ========================

// BenchmarkGet 在预填充 10000 条数据后，随机读取。
func BenchmarkGet(b *testing.B) {
	db, cleanup := openBenchDB(b)
	defer cleanup()

	const seed = 10000
	for i := 0; i < seed; i++ {
		key := strconv.AppendInt([]byte("key-"), int64(i), 10)
		_ = db.Put(key, benchValue)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := strconv.AppendInt([]byte("key-"), int64(i%seed), 10)
		if _, err := db.Get(key); err != nil {
			b.Fatal(err)
		}
	}
}

// ======================== 批量写（Batch） ========================

// BenchmarkBatchPut100 每次 Commit 写入 100 条，测试批量写入吞吐。
func BenchmarkBatchPut100(b *testing.B) {
	db, cleanup := openBenchDB(b)
	defer cleanup()
	const batchSize = 100
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := db.NewBatch()
		batch.Lock()
		for j := 0; j < batchSize; j++ {
			key := []byte(fmt.Sprintf("b%d-k%d", i, j))
			_ = batch.Put(key, benchValue)
		}
		if err := batch.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkBatchPut1000 每次 Commit 写入 1000 条，测试大批量写入吞吐。
func BenchmarkBatchPut1000(b *testing.B) {
	db, cleanup := openBenchDB(b)
	defer cleanup()
	const batchSize = 1000
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := db.NewBatch()
		batch.Lock()
		for j := 0; j < batchSize; j++ {
			key := []byte(fmt.Sprintf("b%d-k%d", i, j))
			_ = batch.Put(key, benchValue)
		}
		if err := batch.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

// ======================== 删除 ========================

// BenchmarkDelete 先批量写入再顺序删除。
func BenchmarkDelete(b *testing.B) {
	db, cleanup := openBenchDB(b)
	defer cleanup()

	for i := 0; i < b.N; i++ {
		key := strconv.AppendInt([]byte("key-"), int64(i), 10)
		_ = db.Put(key, benchValue)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := strconv.AppendInt([]byte("key-"), int64(i), 10)
		_ = db.Delete(key)
	}
}

// ======================== 并发写 / 读 ========================

// BenchmarkPutParallel 使用 b.RunParallel 测试多协程并发写入吞吐。
func BenchmarkPutParallel(b *testing.B) {
	db, cleanup := openBenchDB(b)
	defer cleanup()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := strconv.AppendInt([]byte("pk-"), int64(i), 10)
			_ = db.Put(key, benchValue)
			i++
		}
	})
}

// BenchmarkGetParallel 预填充后，多协程并发随机读取。
func BenchmarkGetParallel(b *testing.B) {
	db, cleanup := openBenchDB(b)
	defer cleanup()

	const seed = 10000
	for i := 0; i < seed; i++ {
		key := strconv.AppendInt([]byte("key-"), int64(i), 10)
		_ = db.Put(key, benchValue)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := strconv.AppendInt([]byte("key-"), int64(i%seed), 10)
			_, _ = db.Get(key)
			i++
		}
	})
}

// BenchmarkMixedReadWrite 模拟 80% 读 + 20% 写的混合负载。
func BenchmarkMixedReadWrite(b *testing.B) {
	db, cleanup := openBenchDB(b)
	defer cleanup()

	const seed = 5000
	for i := 0; i < seed; i++ {
		key := strconv.AppendInt([]byte("key-"), int64(i), 10)
		_ = db.Put(key, benchValue)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%5 == 0 {
				key := strconv.AppendInt([]byte("w-"), int64(i), 10)
				_ = db.Put(key, benchValue)
			} else {
				key := strconv.AppendInt([]byte("key-"), int64(i%seed), 10)
				_, _ = db.Get(key)
			}
			i++
		}
	})
}

// ======================== 启动耗时（索引重建）========================

// BenchmarkReopen 测试写入大量数据后，重启时索引重建的耗时。
// 63573344 ns/op
func BenchmarkReopen(b *testing.B) {
	dir, _ := os.MkdirTemp("./example", "bitcaskdb-reopen-*")
	//defer os.RemoveAll(dir)

	// 一次性写入 10000 条数据
	db, _ := bitcaskdb.Open(&bitcaskdb.Options{DirPath: dir, SegmentSize: bitcaskdb.GB})
	for i := 0; i < 100000; i++ {
		key := strconv.AppendInt([]byte("key-"), int64(i%3000), 10)
		_ = db.Put(key, benchValue)
	}
	_ = db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db2, err := bitcaskdb.Open(&bitcaskdb.Options{DirPath: dir, SegmentSize: bitcaskdb.GB})
		if err != nil {
			b.Fatal(err)
		}
		_ = db2.Close()
	}
}

func BenchmarkMergeReopen(b *testing.B) {
	dir := "./example/bitcaskDB-fixed-nomerge"
	_ = os.MkdirAll(dir, 0766) // 如果目录不存在就创建，存在就啥也不干
	//defer os.RemoveAll(dir)

	// 一次性写入 10000 条数据
	db, _ := bitcaskdb.Open(&bitcaskdb.Options{DirPath: dir, SegmentSize: bitcaskdb.GB})
	for i := 0; i < 100000; i++ {
		key := strconv.AppendInt([]byte("key-"), int64(i%3000), 10)
		_ = db.Put(key, benchValue)
	}

	err := db.Merge()
	if err != nil {
		b.Fatal(err)
	}
	_ = db.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db2, err := bitcaskdb.Open(&bitcaskdb.Options{DirPath: dir, SegmentSize: bitcaskdb.GB})
		if err != nil {
			b.Fatal(err)
		}
		_ = db2.Close()
	}
}
func TestName(t *testing.T) {
	dir := "./example/bitcaskDB-fixed-nomerge"
	db, _ := bitcaskdb.Open(&bitcaskdb.Options{DirPath: dir, SegmentSize: bitcaskdb.GB})
	err := db.Merge()
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
}
