# 从零实现 bitcaskdb

按这个顺序实现，每一步都能单独跑通测试。

## 1. 读懂 WAL 接口

先看 [rosedblabs/wal](https://github.com/rosedblabs/wal) 的接口，理解以下三个操作：

- `Write(data []byte) (*ChunkPosition, error)`：追加写，返回该数据在文件中的坐标
- `Read(pos *ChunkPosition) ([]byte, error)`：按坐标读回数据
- `WriteAll(data [][]byte) ([]*ChunkPosition, error)`：批量追加，一次 IO

## 2. 定义 baseDataStruct 和编解码

定义 Payload 的数据格式（type / batchId / keySize / valueSize / expire / key / value），写好 `encodeBaseDataStruct` 和 `decodeBaseDataStruct`。

## 3. 实现 Batch 的 Put / Delete

把传入的 KV 追加到 `pendingWrite` 列表，同时用 `pendingWriteMap` 快速查重去重。

## 4. 实现 Index

用 google/btree 实现内存索引，接口：`Get / Put / Delete / Size`。不用 map 是因为 map 扩容代价高且内存开销大。

## 5. 实现 Batch.Commit

遍历 `pendingWrite`，编码成 Payload，调用 `WAL.WriteAll` 一次刷盘。末尾追加一条 `Finished` 标记保证原子性。用 WAL 返回的 `ChunkPosition` 更新 index。

## 6. 实现 Batch.Get

先查 pendingWrite，没有再通过 `index.Get(key)` 拿坐标，再 `WAL.Read(pos)` 读数据。

## 7. 实现 DB 层的 Put / Get / Delete

从 batchPool 取 batch，init 后调用对应 batch 方法，Commit，归还 pool。

## 8. 实现 Open

1. 校验 Options
2. 目录不存在则 MkdirAll
3. flock 独占目录
4. 打开 WAL
5. `loadIndex()`（先写个空实现，后面补）

**验证点**：跑通 `Open → Put → Get → Delete` 的基本单测。

## 9. 实现 loadIndexFromWal

遍历 WAL 所有记录：

- 遇到 `Finished` 标记：把该 batchId 对应的暂存记录批量写入 index（Normal → Put，Deleted → Delete）
- 没有 `Finished` 的 batch 数据直接丢弃（崩溃未提交的事务）

**验证点**：Put 若干数据后重启，Get 能恢复。

## 10. 实现 DoMerge

1. `OpenNewActiveSegment()` 轮转 active seg，释放锁
2. 在 `{dir}-merge` 临时目录创建新 WAL 和 hint 文件
3. 遍历 preActiveSeg 以内的数据：
   - 只处理 `Normal` 且未过期的记录
   - `index.Get(key)` 的坐标 == 当前坐标时，写入新 WAL 和 hint
4. 写 MERGEFIN 文件记录 `preActiveSegmentID`

## 11. 实现 ReplaceOriginalFile

1. 从 merge 目录读取 MERGEFIN，获取 `finSegmentId`
2. **先删**原目录的 MERGEFIN 和 HINT（crash 安全关键步骤）
3. 遍历 1 到 finSegmentId：删旧 seg，rename merge seg 过来
4. rename hint 和 MERGEFIN 到原目录

## 12. 实现 loadIndexFromHint

重启时若原目录存在合法 MERGEFIN：

1. 读 hint 文件，把所有 key → position 直接写入 index
2. `loadIndexFromWal` 从 `finSegmentId + 1` 开始扫描，只恢复 merge 之后的增量

**验证点**：大量写入 → merge → 重启，数据完整，index 正确。

## 常见坑

- `newMergeDB` 里复制 Options 必须用 `*db.options`（值拷贝），否则会污染原 db 的 DirPath
- `DoMerge` 读 index 时要持 `db.mu.RLock()`，merge 过程中并发写会修改 index
- `positionEquals` 调用前先判 nil，被删除的 key 在 index 里不存在会直接 panic
- `wal.Open` 失败时 defer Close 会 panic，Open 后先检查 err
