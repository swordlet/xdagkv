package kv

// IKVStore is the interface used by the RDB struct to access the underlying
// Key-Value store.
type IKVStore interface {
	Close() error
	Set(key, value []byte) error
	PSet(keys, values [][]byte) error
	Get(key []byte) ([]byte, bool, error)
	PGet(keys [][]byte) ([][]byte, []bool, error)
	Del(key []byte) (bool, error)
	Keys(pattern []byte, limit int, withvalues bool) ([][]byte, [][]byte, error)
	FlushDB() error
}

func bcopy(b []byte) []byte {
	r := make([]byte, len(b))
	copy(r, b)
	return r
}
