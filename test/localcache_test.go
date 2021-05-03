package test

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/binhgo/go-sdk/sdk"
)

func TestPutLocalCache(t *testing.T) {
	localCache := sdk.NewLCache(100, 5)
	i := 0
	for i < 100 {
		i++
		localCache.Put(strconv.Itoa(i), fmt.Sprintf("Data %d", i))
	}
	fmt.Println(localCache.Len())
	if 100 != localCache.Len() {
		t.Errorf("LocalCache Size = %d , want : 100", localCache.Len())
	}

}

func TestExpiredLocalCache(t *testing.T) {
	localCache := sdk.NewLCache(100, 5)
	i := 0
	for i < 100 {
		i++
		localCache.Put(strconv.Itoa(i), fmt.Sprintf("Data %d", i))
	}
	time.Sleep(6 * time.Second)

	if 0 != localCache.Len() {
		t.Errorf("LocalCache Size = %d , want : 0", localCache.Len())
	}
}

func TestExpiredLocalCache2(t *testing.T) {
	localCache := sdk.NewLCache(100, 5)
	i := 0
	for i < 100 {
		i++
		localCache.Put(strconv.Itoa(i), fmt.Sprintf("Data %d", i))
	}
	time.Sleep(6 * time.Second)

	localCache.Put(strconv.Itoa(10), fmt.Sprintf("Data %d", 10))

	if 1 != localCache.Len() {
		t.Errorf("LocalCache Size = %d , want : 1", localCache.Len())
	}
}

func TestExpiredWithGet(t *testing.T) {
	localCache := sdk.NewLCache(100, 5)
	i := 0
	for i < 10 {
		i++
		localCache.Put(strconv.Itoa(i), fmt.Sprintf("Data %d", i))
	}
	time.Sleep(3 * time.Second)
	localCache.Get("1")
	time.Sleep(3 * time.Second)

	if 1 != localCache.Len() {
		t.Errorf("LocalCache Size = %d , want : 1", localCache.Len())
	}
}

func TestExpiredWithContainsKey(t *testing.T) {
	localCache := sdk.NewLCache(100, 5)
	i := 0
	for i < 10 {
		i++
		localCache.Put(strconv.Itoa(i), fmt.Sprintf("Data %d", i))
	}
	time.Sleep(3 * time.Second)
	localCache.ContainsKey("1")
	time.Sleep(3 * time.Second)

	if 1 != localCache.Len() {
		t.Errorf("LocalCache Size = %d , want : 1", localCache.Len())
	}
}

func TestGetValueNotFound(t *testing.T) {
	localCache := sdk.NewLCache(100, 5)

	val, ok := localCache.Get("200")

	if ok {
		t.Errorf("LocalCache Value with key 200  = %s , want : not found", val)
	}
}

func TestGetValueSuccess(t *testing.T) {
	localCache := sdk.NewLCache(100, 5)
	localCache.Put("200", "400")

	val, ok := localCache.Get("200")

	if !ok {
		t.Errorf("LocalCache Value with key 200 not found , want : %s", val)
	}
}

func TestContain(t *testing.T) {
	localCache := sdk.NewLCache(100, 5)
	localCache.Put("200", "400")

	ok := localCache.ContainsKey("200")

	if !ok {
		t.Errorf("LocalCache Value with key 200 not found , want : existed")
	}
}

func TestNotContain(t *testing.T) {
	localCache := sdk.NewLCache(100, 5)
	ok := localCache.ContainsKey("200")

	if ok {
		t.Errorf("LocalCache Value with key 200 existed , want : not found ")
	}
}

func TestExpiredRefreshModeWithGet(t *testing.T) {
	localCache := sdk.NewLCacheRefreshMode(100, 5, false)
	i := 0
	for i < 10 {
		i++
		localCache.Put(strconv.Itoa(i), fmt.Sprintf("Data %d", i))
	}
	time.Sleep(3 * time.Second)
	localCache.Get("1")
	time.Sleep(3 * time.Second)

	if 0 != localCache.Len() {
		t.Errorf("LocalCache Size = %d , want : 0", localCache.Len())
	}
}

func TestExpiredRefreshModeWithContainsKey(t *testing.T) {
	localCache := sdk.NewLCacheRefreshMode(100, 5, false)
	i := 0
	for i < 10 {
		i++
		localCache.Put(strconv.Itoa(i), fmt.Sprintf("Data %d", i))
	}
	time.Sleep(3 * time.Second)
	localCache.ContainsKey("1")
	time.Sleep(3 * time.Second)

	if 0 != localCache.Len() {
		t.Errorf("LocalCache Size = %d , want : 0", localCache.Len())
	}
}
