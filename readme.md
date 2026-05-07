## 整体流程
### 数据结构：
1. 最外面的是000001.seg，000002.seg...

2. 每一个seg里面都分block块。每一个block块默认32k。对wal而言，最基础的数据是存放在block中的

```
       +-----+-------------+--+----+----------+------+-- ... ----+
 File  | r0  |      r1     |P | r2 |    r3    |  r4  |           |
       +-----+-------------+--+----+----------+------+-- ... ----+
       |<---- BlockSize ----->|<---- BlockSize ----->|

  rn = variable size records
  P = Padding
  BlockSize = 32KB
```
3. wal中最基础的数据结构(**Chunk**)如下图。crc是校验和，length的payload的长度，type是类型
如果某个数据过大，一个block放不下，type用于区分头尾。数据库的数据存放在payload中
```
+----------+-------------+-----------+--- ... ---+
| CRC (4B) | Length (2B) | Type (1B) |  Payload  |
+----------+-------------+-----------+--- ... ---+

CRC = 32-bit hash computed over the payload using CRC
Length = Length of the payload data
Type = Type of record
       (FullType, FirstType, MiddleType, LastType)
       The type is used to group a bunch of records together to represent
       blocks that are larger than BlockSize
Payload = Byte stream as long as specified by the payload size
```
4. 对数据库而言，payload中存放的就是db中的数据了，在put的时候，每一条数据都是一个**Chunk**的payload，每一个数据都会被single record包裹，
```
 +-------------+-------------+-------------+--------------+---------------+---------+--------------+
 |    type     |  batch id   |   key size  |   value size |     expire    |  key    |      value   |
 +-------------+-------------+-------------+--------------+---------------+--------+--------------+

	1 byte	      varint(max 10) varint(max 5)  varint(max 5) varint(max 10)  varint      varint

```
## 整体的逻辑流程：
1. 用户先打开数据库，调用open
	1. open会调用wal的open，获取最新的seg的句柄
2. 调用put
	1. put会调用batch的put和commit
		1. batch是一个批处理的工具，
		2. batch.put: 把传入的kv直接存在batch结构的一个list：pendingWrite中,没了，加一个判断，如果list中有这个数据就更新
		3. batch.commit:遍历pendingWrite，把数据解析成payload需要的格式并调用wal的PendingWrites加进去。然后加一条结束的数据标识用来保证原子性。最后调用wal的writeAll一次写进去，并把返回的坐标存起来，这里是存到了**indexer**中
3. 调用get
	1. get会调用batch的get
		1. batch的get先检查list里有没有，有就直接返回
		2. 如果没有就得根据key去indexer获取坐标，然后调用wal的read获取数据
## 复刻流程

1. 先看wal的接口以及实现（解析在xxx.md里）
	1. wal的write：如果当前block已经连Chunk的header都放不下了，就补0至下一个block。如果放得下头但放不下整个数据，就会拆分存到多个block中，然后返回这个数据的最前面的block的坐标（segId,blockSize,blockOffset，ChunkSize）
	2. wal的read：根据这3个segId,blockSize,blockOffset读取出数据并拼接返回payload
	3. writeAll:本质就是wal有个list集合pendingWrites，平时用特定的write方法会把数据写到pendingWrites里。调用writeAll的时候一次io把整个list都写到文件里。
2. 把batch的put，delete写了，然后写commit
3. 写commit的时候你会发现需要一个数据结构来保存wal返回的坐标，这就是前面提到的indexer，这里选择很多，只要能做到indexer.get(key)能返回坐标的数据结构都可以，但go的话最好不要map，map一个是容易触发扩容，一个是本身go维护的map也不小。项目里用的是google的btree
4. 把indexer给实现了。get，put，delete，size，Iterator(这个比较复杂，可以后面再弄)
5. 回过头把batch的get，commit给写了