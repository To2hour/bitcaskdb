package bitcaskdb

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestDbTest(t *testing.T) {
	db, _ := Open(DbDefaultOptions)

	var wg sync.WaitGroup
	var wg1 sync.WaitGroup
	ch := make(chan string)

	// 先写完
	wg.Add(10000)
	for i := range 10000 {
		i := i
		go func() {
			defer wg.Done()
			_ = db.Put([]byte("key"+strconv.Itoa(i)), []byte("value"+strconv.Itoa(i)))
		}()
	}
	wg.Wait()

	// 再读完
	wg1.Add(10000)
	var res []string
	for i := range 10000 {
		i := i
		go func() {
			defer wg1.Done()
			data, err := db.Get([]byte("key" + strconv.Itoa(i)))
			if err == nil {
				//fmt.Println(string(data))
				ch <- string(data)
			}
		}()
	}
	go func() {
		for data := range ch {
			res = append(res, data)
		}
	}()
	wg1.Wait()
	close(ch)
	fmt.Println(len(res))
}
func TestOpen(t *testing.T) {
	start := time.Now()
	//没hit辅助，打开耗时 Open 函数耗时: 64.8956ms
	//没hit辅助，打开耗时 Open 函数耗时: 65.9413ms
	//没hit辅助，打开耗时 Open 函数耗时: 64.8681ms
	//没hit辅助，打开耗时 Open 函数耗时: 67.4314ms
	//没hit辅助，打开耗时 Open 函数耗时: 64.8956ms
	db, _ := Open(DbDefaultOptions)
	elapsed := time.Since(start)
	t.Logf("Open 函数耗时: %s\n", elapsed) // 或者 fmt.Printf
	size := db.index.Size()
	t.Logf("总数据量 : %d", size) // 或者 fmt.Printf
	//    db_test.go:61: Open 函数耗时: 63.9886ms
	//    db_test.go:63: 总数据量 : 10000
	//    db_test.go:61: Open 函数耗时: 65.8938ms
	//    db_test.go:63: 总数据量 : 10000
	//for {
	//	if iterator.Valid() {
	//		fmt.Print(string(iterator.Key()), "-->")
	//		val, _ := db.dataFiles.Read(iterator.Value())
	//		dataStruct := decodeBaseDataStruct(val)
	//		fmt.Println(string(dataStruct.Value))
	//	} else {
	//		break
	//	}
	//	iterator.Next()
	//}
	//iterator.Close()
}
