package bitcaskdb

// merge 把wal中那些老的seg段合并成更少个
// todo思路是什么？
// 从wal中new reader。当遍历到的seg == active的时候就停止
// 然后把reader读到的数据解码，类似loadIndex后
// put到索引里，然后索引猛猛put后，检测下大小，差不多
// 和seg设定的大小一样了就commit进去，不过要不然改commit，要不然写个新的commit
// 问题是怎么覆盖原文件？然后怎么替换？

// todo roseDb思路：创造一个临时文件夹，把目前active的seg往后一位，然后read除了新的之外所有的seg
// 然后根据delete，过期时间等标记只把有用的数据放到新的seg中
// 然后用rename替换掉老的seg(遍历新的seg然后直接改名过去替换即可todo（需要注意老的没用的seg得干掉）)
// 然后替换结束后重新加载索引，就ok了
func (db *DB) merge() {

}

//todo 到时候写完merge，写一个基于lru的内存淘汰机制，让indexer别保存全量索引
