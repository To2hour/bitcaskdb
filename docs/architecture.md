# 架构设计

## WAL 文件结构

数据存储在一组 seg 文件中（`000001.SEG`, `000002.SEG` ...），每个 seg 按 32KB 切成 Block：

```
       +-----+-------------+--+----+----------+------+-- ... ----+
 File  | r0  |      r1     |P | r2 |    r3    |  r4  |           |
       +-----+-------------+--+----+----------+------+-- ... ----+
       |<---- BlockSize ----->|<---- BlockSize ----->|

  rn = variable size records
  P  = Padding（block 尾部不足以放 header 时补 0）
  BlockSize = 32KB
```

## Chunk 格式

Block 内每条数据封装为 Chunk。CRC 用于校验，Type 用于处理跨 block 的大数据：

```
+----------+-------------+-----------+--- ... ---+
| CRC (4B) | Length (2B) | Type (1B) |  Payload  |
+----------+-------------+-----------+--- ... ---+

Type: FullType | FirstType | MiddleType | LastType
```

## Payload（baseDataStruct）格式

Chunk 的 Payload 即数据库的一条记录：

```
+------+-----------+----------+------------+---------+-------+-------+
| type | batch id  | key size | value size | expire  |  key  | value |
+------+-----------+----------+------------+---------+-------+-------+
  1B    varint(10)  varint(5)   varint(5)   varint(10)  ...    ...
```

hint 文件和 mergefin 文件结构与此类似，区别仅在 Payload 内容不同。

## 代码分层

### DB

整个数据库只有 1 个 DB 实例，持有：

- `dataFiles`：WAL 句柄，对应所有 `.SEG` 文件
- `index`：内存 B-tree，存 Key → `*wal.ChunkPosition`
- `batchPool`：复用 Batch 对象，减少 GC 压力
- `mu`：读写锁，保护 index 和 dataFiles 的并发访问

### Batch

一个 DB 可以同时存在多个 Batch。Batch 分只读和读写两种：

- `pendingWrite`（有序列表）+ `pendingWriteMap`（快速查重）：commit 前的暂存
- commit 前的 Get/Put/Delete 只操作 pendingWrite，不同 Batch 完全隔离
- commit 时遍历 pendingWrite → 编码为 Payload → 调用 WAL 的 `WriteAll` 一次刷盘 → 更新 index
- 末尾写一条 `Finished` 标记保证原子性；重启恢复时没有 `Finished` 的批次直接丢弃

### Index

内存 B-tree（google/btree），接口：`Get(key) / Put(key, pos) / Delete(key) / Size() / Iterator()`。

不用 map 的原因：map 扩容代价高，且 Go 内置 map 本身有不小的内存开销。

## 操作流程

### Open

1. 校验 Options，目录不存在则创建
2. 用 flock 独占目录（同一目录只能有一个 DB 实例）
3. 打开 WAL（`openDataWal`）
4. 加载索引（`loadIndex`）

### Put / Delete

```
db.Put(key, value)
  └─ batch.init(readOnly=false)
  └─ batch.Put(key, value)       → 写入 pendingWrite
  └─ batch.Commit()
       ├─ 遍历 pendingWrite，编码为 Payload
       ├─ WAL.WriteAll()          → 一次刷盘
       └─ 更新 index
```

### Get

```
db.Get(key)
  └─ batch.init(readOnly=true)
  └─ batch.Get(key)
       ├─ 先查 pendingWrite（batch 内可见性）
       └─ index.Get(key) → WAL.Read(pos) → 返回 value
```

### Merge

1. `OpenNewActiveSegment()`：把当前 active seg 轮转到下一个，新写入不影响 merge
2. 在临时目录（`{dir}-merge`）创建新的 WAL
3. 遍历 preActiveSeg 以内的所有数据，只保留 `index.Get(key) == 当前位置` 的有效数据写入新 WAL，并写 hint 文件
4. 写 `MERGEFIN` 文件记录本次 merge 涉及的最大 seg ID
5. 锁住 DB，关闭原 WAL，将临时目录的文件 rename 到正式目录，重新打开 WAL 并重建 index

**Crash 安全**：rename 前先删掉旧的 MERGEFIN 和 HINT，即使 rename 中途崩溃，重启时因找不到 MERGEFIN 会回退到全量 WAL 扫描，不会出现索引错乱。

### 重启索引恢复（loadIndex）

```
loadIndexFromHint()   ← 若存在合法 MERGEFIN，用 hint 快速恢复 merge 范围内的索引
loadIndexFromWal()    ← 从 finSegmentId+1 开始扫描，恢复 merge 之后的增量写入
```
