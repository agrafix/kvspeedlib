package kvspeedlib

import (
	"fmt"
	"github.com/fzzy/radix/redis"
	"strconv"
	"strings"
	"time"
)

type PersistValueAction func(Key uint64)

type KVStoreAction struct {
	Key   uint64
	Owner uint64
	Value []byte
	Fun   PersistValueAction
}

type KVBase struct {
	name       string
	redisNet   string
	redisAddr  string
	cacheTtl   time.Duration
	workQueue  chan *KVStoreAction
	storeQueue chan *KVStoreAction
}

func Open(name string, redisNetwork, redisAddr string, cacheTtl time.Duration) (*KVBase, error) {
	base := KVBase{
		name:       name,
		redisNet:   redisNetwork,
		redisAddr:  redisAddr,
		cacheTtl:   cacheTtl,
		workQueue:  make(chan *KVStoreAction, 100),
		storeQueue: make(chan *KVStoreAction, 1000),
	}

	go worker(&base)
	go storeWorker(&base)

	return &base, nil
}

func storeWorker(base *KVBase) {
	for {
		storeAction := <-base.storeQueue
		storeAction.Fun(storeAction.Key)
	}
}

func (base *KVBase) mkOwnerKey(owner uint64) string {
	return base.name + "_" + fmt.Sprintf("%x", owner)
}

func (base *KVBase) mkValueKey(key uint64) string {
	return base.name + "_" + strconv.FormatUint(key, 10)
}

func worker(base *KVBase) {
	c, err := redis.DialTimeout(base.redisNet, base.redisAddr, time.Duration(10)*time.Second)
	if err != nil {
		panic(err)
	}
	defer c.Close()

	for {
		storeAction := <-base.workQueue
		bytes := storeAction.Value

		ownerKey := base.mkOwnerKey(storeAction.Owner)

		r := c.Cmd("INCR", ownerKey+"_idx")
		if r.Err != nil {
			panic(r.Err)
		}
		nextKey, _ := r.Int64()
		storeAction.Key = uint64(nextKey)

		now := time.Now().Format("20060102150405")
		valueKey := base.mkValueKey(storeAction.Key)
		c.Cmd("ZADD", ownerKey+"_set", now, valueKey)
		c.Cmd("EXPIRE", ownerKey+"_set", uint64(base.cacheTtl.Seconds()+5))

		c.Cmd("SET", valueKey, bytes)
		c.Cmd("EXPIRE", valueKey, uint64(base.cacheTtl.Seconds()))
		base.storeQueue <- storeAction
	}
}

func (base *KVBase) StoreValue(owner uint64, value []byte, persistFun PersistValueAction) {
	storeAction := KVStoreAction{
		Key:   0,
		Owner: owner,
		Value: value,
		Fun:   persistFun,
	}
	base.workQueue <- &storeAction
}

func (base *KVBase) LoadValues(owner uint64, limit int) (uint64, [][]byte) {
	c, err := redis.DialTimeout(base.redisNet, base.redisAddr, time.Duration(10)*time.Second)
	if err != nil {
		panic(err)
	}

	ownerKey := base.mkOwnerKey(owner)
	tooOld := time.Now().Add(-1 * base.cacheTtl).Format("20060102150405")
	c.Cmd("ZREMRANGEBYSCORE", ownerKey+"_set", 0, tooOld)
	r := c.Cmd("ZREVRANGEBYSCORE",
		ownerKey+"_set", "+inf", tooOld, "LIMIT", "0", limit)
	if r.Err != nil {
		fmt.Println("RECV FAILED")
		panic(r.Err)
	}

	values, _ := r.List()
	datasets := make([][]byte, 0, 0)
	var minKey uint64 = 18446744073709551615
	for _, valueKeyStr := range values {
		valueKey, _ := strconv.ParseUint(strings.Replace(valueKeyStr, base.name+"_", "", -1), 10, 64)
		minKey = valueKey
		r := c.Cmd("GET", valueKeyStr)
		if r.Err != nil {
			panic(r.Err)
		}

		valueBytes, err := r.Bytes()
		if err != nil {
			panic(err)
		}
		datasets = append(datasets, valueBytes)
	}
	return minKey, datasets
}
