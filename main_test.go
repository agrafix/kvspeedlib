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

type DummyStoreEngine struct {
	Data map[uint64]map[uint64]TestStoreVal
}

func (se DummyStoreEngine) Save(owner uint64, key uint64, val TestStoreVal) {
	_, ok := se.Data[owner]
	if !ok {
		se.Data[owner] = make(map[uint64]TestStoreVal)
	}
	se.Data[owner][key] = val
}

func (se DummyStoreEngine) MultiLoad(maxKey uint64, owner uint64, limit int) []interface{} {
	ownersData := se.Data[owner]
	ct := 0

	interfs := make([]interface{}, 0, 0)
	for k, v := range ownersData {
		if k < maxKey && ct <= limit {
			interfs = append(interfs, v)
			ct++
		}
	}

	return interfs
}

var (
	DummyStore DummyStoreEngine
	OwnerCt    uint64
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
	DummyStore = DummyStoreEngine{
		Data: make(map[uint64]map[uint64]TestStoreVal),
	}
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
		DummyStore.Save(owner1, key, value)
		savedChan <- true
	})

	out := <-savedChan
	if !out {
		t.Error("Never saved anything")
	}

	_, foundItems := kvstore.LoadValues(owner1, 100)
	if len(foundItems) != 1 {
		t.Errorf("LoadValues didn't return the saved key. Count: %d", len(foundItems))
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
		DummyStore.Save(owner1, key, value)
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
