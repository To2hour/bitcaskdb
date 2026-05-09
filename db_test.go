package bitcaskdb

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
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
	db, _ := Open(DbDefaultOptions)
	iterator := db.index.Iterator(false)
	for {
		if iterator.Valid() {
			fmt.Print(string(iterator.Key()), "-->")
			val, _ := db.dataFiles.Read(iterator.Value())
			dataStruct := decodeBaseDataStruct(val)
			fmt.Println(string(dataStruct.Value))
		} else {
			break
		}
		iterator.Next()
	}
	iterator.Close()
}
