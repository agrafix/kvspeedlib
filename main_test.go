package kvspeedlib_test

import (
	"encoding/json"
	"github.com/agrafix/kvspeedlib"
	"strconv"
	"testing"
	"time"
)

type TestStoreVal struct {
	Foo string
}

var (
	OwnerCt uint64
)

func mkOwner() uint64 {
	nowStr := time.Now().Format("20060102150405")
	OwnerCt++
	r, _ := strconv.ParseUint(nowStr+strconv.FormatUint(OwnerCt, 10), 10, 64)
	return r
}

func mkKvStore(name string, secs int) *kvspeedlib.KVBase {
	kvStore, err := kvspeedlib.Open(name, "tcp", ":6379", time.Duration(secs)*time.Second)
	if err != nil {
		panic(err)
	}
	return kvStore
}

func init() {
	OwnerCt = 0
}

func TestAdd(t *testing.T) {
	owner1 := mkOwner()
	owner2 := mkOwner()
	kvstore := mkKvStore("TestAdd", 10)

	value := TestStoreVal{
		Foo: "bar",
	}
	savedChan := make(chan bool, 1)
	jsonBytes, _ := json.Marshal(value)
	kvstore.StoreValue(owner1, jsonBytes, func(key uint64) {
		savedChan <- true
	})

	out := <-savedChan
	if !out {
		t.Error("Never saved anything")
	}

	minKey, foundItems := kvstore.LoadValues(owner1, 100)
	if len(foundItems) != 1 {
		t.Errorf("LoadValues didn't return the saved key. Count: %d", len(foundItems))
	}
	if minKey != 1 {
		t.Errorf("LoadValues returned a wrong minKey: %d", minKey)
	}

	_, foundItems2 := kvstore.LoadValues(owner2, 100)
	if len(foundItems2) != 0 {
		t.Errorf("The other owner has some data? Count: %d", len(foundItems))
	}
}

func TestExpire(t *testing.T) {
	owner1 := mkOwner()
	kvstore := mkKvStore("TestExpire", 0)

	value := TestStoreVal{
		Foo: "bar",
	}
	savedChan := make(chan bool, 1)
	jsonBytes, _ := json.Marshal(value)
	kvstore.StoreValue(owner1, jsonBytes, func(key uint64) {
		savedChan <- true
	})

	out := <-savedChan
	if !out {
		t.Error("Never saved anything")
	}

	_, foundItems := kvstore.LoadValues(owner1, 100)
	if len(foundItems) != 0 {
		t.Errorf("LoadValues didn't return the saved key. %d", len(foundItems))
	}
}
