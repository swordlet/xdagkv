package main

import (
	"crypto/sha256"
	"flag"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/swordlet/xdagkv/kv"
	"github.com/swordlet/xdagkv/xdag"
)

var (
	// duration = flag.Duration("d", time.Minute, "test duration for each case")
	c     = flag.Int("c", runtime.GOMAXPROCS(-1), "concurrent goroutines")
	fsync = flag.Bool("fsync", true, "fsync")
	s     = flag.String("s", "leveldb", "store type")
)

func main() {

	var startTime, endTime uint64
	// startTime = 0x16c37240000
	// endTime = 0x17240810000
	startTime = 0x16ef93e0000
	endTime = 0x17240810000
	start := time.Now()
	blocks, err := xdag.LoadBlocks(startTime, endTime)
	if err != nil {
		panic(err)
	}
	dur := time.Since(start)
	d := int64(dur)
	dataLen := len(blocks)
	fmt.Printf("%s single thread get rate: %d op/s, mean: %d ns, took: %d s, kv total: %d\n", "xdag file store",
		int64(dataLen)*1e6/(d/1e3), d/int64(dataLen), int(dur.Seconds()), dataLen)

	flag.Parse()

	// fmt.Printf("duration=%v, c=%d\n", *duration, *c)
	fmt.Printf("CPU core number = %d\n", *c)
	fmt.Printf("key length: %d, value length: %d\n", len(blocks[0].Hash), len(blocks[0].RawBytes))
	cores := *c

	var path string

	store, path, err := getStore(*s, *fsync, path)
	if err != nil {
		panic(err)
	}

	name := *s
	counts := make([]int, cores)
	count := dataLen / cores
	for i := range counts {
		counts[i] = count
	}
	fmt.Println("goroutine tasks: ", counts)
	n := count * cores
	fmt.Println("total: ", n)

	// test set
	{
		var wg sync.WaitGroup
		wg.Add(cores)

		start := time.Now()
		for j := 0; j < cores; j++ {
			go func(j int) {
				i := j
				for k := 0; k < counts[j]; k++ {
					store.Set(blocks[i].Hash[:], blocks[i].RawBytes)
					i += cores
				}
				wg.Done()
			}(j)
		}
		wg.Wait()
		dur := time.Since(start)
		d := int64(dur)
		fmt.Printf("%s set rate: %d op/s, mean: %d ns, took: %d s, kv total: %d\n",
			name, int64(n)*1e6/(d/1e3), d/int64(n), int(dur.Seconds()), n)
	}

	// test get
	{
		var wg sync.WaitGroup
		wg.Add(cores)
		var fails = make([]bool, cores)
		start := time.Now()
		for j := 0; j < cores; j++ {
			go func(j int) {
				i := j
				for k := 0; k < counts[j]; k++ {
					_, ok, _ := store.Get(blocks[i].Hash[:])
					if !ok {
						fails[j] = true
						break
					}
					i += cores
				}
				wg.Done()
			}(j)
		}
		wg.Wait()
		dur := time.Since(start)
		d := int64(dur)
		// fmt.Println(d)
		fmt.Printf("%s get rate: %d op/s, mean: %d ns, took: %d s, kv total: %d\n",
			name, int64(n)*1e6/(d/1e3), d/int64(n), int(dur.Seconds()), n)
		fmt.Println("failed goroutine: ", fails)
	}

	// test multiple get/one set
	{
		keys := genKey(dataLen)
		var data = make([]byte, 512)
		var fails = make([]bool, cores)

		var wg sync.WaitGroup
		wg.Add(cores)

		ch := make(chan struct{})

		var setCount uint64

		go func() {
			i := 0
			for {
				select {
				case <-ch:
					return
				default:
					store.Set(keys[i][:], data)
					setCount++
					i++
				}
			}
		}()

		start := time.Now()
		for j := 0; j < cores; j++ {
			go func(j int) {
				i := j
				for k := 0; k < counts[j]; k++ {
					_, ok, _ := store.Get(blocks[i].Hash[:])
					if !ok {
						fails[j] = true
						break
					}
					i += cores
				}
				wg.Done()
			}(j)
		}
		wg.Wait()
		close(ch)
		dur := time.Since(start)
		d := int64(dur)

		fmt.Println("set total: ", setCount)
		if setCount == 0 {
			fmt.Printf("%s setmixed rate: -1 op/s, mean: -1 ns, took: %d s\n", name, int(dur.Seconds()))
		} else {
			fmt.Printf("%s setmixed rate: %d op/s, mean: %d ns, took: %d s\n", name, int64(setCount)*1e6/(d/1e3), d/int64(setCount), int(dur.Seconds()))
		}
		fmt.Printf("%s getmixed rate: %d op/s, mean: %d ns, took: %d s\n", name, int64(n)*1e6/(d/1e3), d/int64(n), int(dur.Seconds()))
		fmt.Println("failed goroutine: ", fails)

	}

	// test del
	{
		var wg sync.WaitGroup
		wg.Add(cores)

		start := time.Now()
		for j := 0; j < cores; j++ {
			go func(j int) {
				i := j
				for k := 0; k < counts[j]; k++ {
					store.Del(blocks[i].Hash[:])
					i += cores
				}
				wg.Done()
			}(j)
		}
		wg.Wait()
		dur := time.Since(start)
		d := int64(dur)
		fmt.Printf("%s del rate: %d op/s, mean: %d ns, took: %d s\n", name, int64(n)*1e6/(d/1e3), d/int64(n), int(dur.Seconds()))
	}
}

func genKey(i int) (keys [][32]byte) {
	for k := 0; k < i; k++ {
		b := []byte(strconv.Itoa(k))
		hash := sha256.Sum256(b)
		keys = append(keys, sha256.Sum256(hash[:]))
	}
	return keys
}

func getStore(s string, fsync bool, path string) (kv.IKVStore, string, error) {
	var store kv.IKVStore
	var err error
	switch s {
	default:
		err = fmt.Errorf("unknown store type: %v", s)
	case "leveldb":
		if path == "" {
			path = "leveldb.db"
		}
		store, err = kv.NewLevelDBStore(path, fsync)
	case "lmdb":
		if path == "" {
			path = "lmdb.db"
		}
		store, err = kv.NewLmdbStore(path, fsync)
	case "rocksdb":
		if path == "" {
			path = "rocksdb.db"
		}
		store, err = kv.NewRocksdbStore(path, fsync)
	}

	return store, path, err
}
