package kv

import (
	"bytes"
	"log"

	"example.com/xdagkv/util"

	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/bmatsuo/lmdb-go/lmdbscan"
)

type lmdbStore struct {
	env   *lmdb.Env
	dbi   lmdb.DBI
	fsync bool
}

// NewLmdbStore returns a lmdb based IKVStore instance.
func NewLmdbStore(path string, fsync bool) (IKVStore, error) {
	env, err := lmdb.NewEnv()
	if err != nil {
		log.Fatalf("cannot create LMDB env (%v)", err)
	}
	err = env.SetMaxDBs(1)
	if err != nil {
		log.Fatalf("cannot set max DBs  (%v)", err)
	}
	err = env.SetMapSize(1 << 30)
	if err != nil {
		log.Fatalf("cannot set Map Size (%v)", err)
	}
	if ok, _ := util.Exist(path); !ok {
		util.Mkdir(path)
	}
	// Flags for Env.Open.
	// FixedMap   Danger zone. Map memory at a fixed address.
	// Readonly   Used in several functions to denote an object as readonly.
	// WriteMap   Use a writable memory map.
	// NoMetaSync Don't fsync metapage after commit.
	// NoSync     Don't fsync after commit.
	// MapAsync   Flush asynchronously when using the WriteMap flag.
	if fsync {
		err = env.Open(path, 0, 0755)
	} else {
		err = env.Open(path, lmdb.NoSync, 0755)
	}
	if err != nil {
		env.Close()
		log.Fatalf("cannot create dir for RDB WAL (%v)", err)
	}

	// Clear stale readers
	stalereads, err := env.ReaderCheck()
	if err != nil {
		log.Fatalf("%v", err)
	}
	if stalereads > 0 {
		log.Printf("cleared %d reader slots from dead processes\n", stalereads)
	}

	// load lmdb
	var dbi lmdb.DBI
	err = env.Update(func(txn *lmdb.Txn) (er error) {
		dbi, er = txn.CreateDBI("xdagbench")
		return er
	})
	if err != nil {
		log.Fatalf("%v", err)
	}
	return &lmdbStore{env, dbi, fsync}, nil
}

// Name returns the IKVStore type name.
func (s *lmdbStore) Name() string {
	return "lmdb"
}

func (s *lmdbStore) Close() error {
	if s.env != nil {
		s.env.CloseDBI(s.dbi)
		err := s.env.Close()
		if err != nil {
			log.Fatalf("Close fail ()%v", err)
		}
	}
	return nil
}

func (s *lmdbStore) Get(key []byte) ([]byte, bool, error) {
	var v []byte
	err := s.env.View(func(txn *lmdb.Txn) (er error) {
		v, er = txn.Get(s.dbi, key)
		return er
	})
	return v, v != nil, err
}

func (s *lmdbStore) PGet(keys [][]byte) ([][]byte, []bool, error) {
	var vals = make([][]byte, len(keys))
	var oks = make([]bool, len(keys))
	err := s.env.View(func(txn *lmdb.Txn) (er error) {
		for i := range keys {
			vals[i], er = txn.Get(s.dbi, keys[i])
			oks[i] = er == nil
		}
		return er
	})
	return vals, oks, err
}

func (s *lmdbStore) Set(key, value []byte) error {
	err := s.env.Update(func(txn *lmdb.Txn) error {
		er := txn.Put(s.dbi, key, value, 0)
		return er
	})
	return err
}

func (s *lmdbStore) PSet(keys, vals [][]byte) error {
	err := s.env.Update(func(txn *lmdb.Txn) (er error) {
		for i, k := range keys {
			er = txn.Put(s.dbi, k, vals[i], 0)
			if er != nil {
				return er
			}
		}
		return nil
	})
	return err
}

func (s *lmdbStore) Del(key []byte) (bool, error) {
	err := s.env.Update(func(txn *lmdb.Txn) error {
		er := txn.Del(s.dbi, key, nil)
		return er
	})
	return err == nil, err
}

func (s *lmdbStore) Keys(pattern []byte, limit int,
	withvals bool) ([][]byte, [][]byte, error) {

	var keys [][]byte
	var vals [][]byte
	err := s.env.View(func(txn *lmdb.Txn) error {
		scanner := lmdbscan.New(txn, s.dbi)
		defer scanner.Close()

		scanner.Set(pattern, nil, lmdb.SetRange)
		for scanner.Scan() {
			if !bytes.HasPrefix(scanner.Key(), pattern) {
				break
			}
			keys = append(keys, scanner.Key())
			if withvals {
				vals = append(vals, scanner.Val())
			}
		}
		return scanner.Err()
	})

	return keys, vals, err
}

func (s *lmdbStore) FlushDB() error {
	// LMDB always flushes the OS buffers upon commit as well
	if s.fsync {
		return nil
	}
	return s.env.Sync(true)
}
