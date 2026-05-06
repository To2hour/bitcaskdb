package util

import "github.com/cespare/xxhash/v2"

func ByteHash(key []byte) uint64 {
	return xxhash.Sum64(key)
}
func StringHash(key string) uint64 {
	return xxhash.Sum64String(key)
}
