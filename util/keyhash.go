package util

import "hash/fnv"

func HashBytesKey(bs []byte) int {
	h := fnv.New32a()
	h.Write(bs)
	return int(h.Sum32() & 0x7fffffff)
}

func HashStringKey(s string) int {
	return HashBytesKey([]byte(s))
}
