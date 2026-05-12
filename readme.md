# bitcaskdb

基于 [Bitcask](https://riak.com/assets/bitcask-a-log-structured-hash-table-for-fast-key-value-data.pdf) 论文实现的 KV 存储引擎，底层使用 [rosedblabs/wal](https://github.com/rosedblabs/wal) 作为 WAL 实现。

## 快速开始

```go
db, err := bitcaskdb.Open(&bitcaskdb.Options{
    DirPath:     "./mydb",
    SegmentSize: bitcaskdb.GB,
})
if err != nil {
    panic(err)
}
defer db.Close()

// 写入
_ = db.Put([]byte("hello"), []byte("world"))

// 读取
val, _ := db.Get([]byte("hello"))
fmt.Println(string(val)) // world

// 删除
_ = db.Delete([]byte("hello"))
```

## 架构概述

数据分三层组织：

```
数据库目录
├── 000001.SEG, 000002.SEG ...  ← WAL 数据文件（seg）
├── 000001.HINT                 ← merge 后的索引快照
└── 000001.MERGEFIN             ← merge 完成标记
```

- **WAL**：所有写入追加到 seg 文件，seg 内部按 32KB Block 分块，每条数据包裹为 Chunk
- **Index**：内存 B-tree，Key → WAL 坐标（segId + blockNumber + offset）
- **Batch**：写操作先缓存到 pendingWrite，commit 时一次性刷盘并更新 index
- **Merge**：压缩历史 seg，去除无效数据，生成 hint 文件加速重启时的索引恢复

详细设计见 [docs/architecture.md](docs/architecture.md)。

从零实现的步骤见 [docs/tutorial.md](docs/tutorial.md)。
