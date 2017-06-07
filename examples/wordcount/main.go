package main

import (
	"strconv"
	"strings"
	"unicode"

	"github.com/naturali/kmr/executor"
	kmrpb "github.com/naturali/kmr/pb"
)

func Map(kvs <-chan *kmrpb.KV) <-chan *kmrpb.KV {
	out := make(chan *kmrpb.KV, 1024)
	go func() {
		for kv := range kvs {
			for _, key := range strings.FieldsFunc(string(kv.Value), func(c rune) bool {
				return !unicode.IsLetter(c)
			}) {
				out <- &kmrpb.KV{Key: []byte(key), Value: []byte(strconv.Itoa(1))}
			}
		}
		close(out)
	}()
	return out
}

func Reduce(kvs <-chan *kmrpb.KV) <-chan *kmrpb.KV {
	out := make(chan *kmrpb.KV, 1024)
	go func() {
		count := 0
		word := ""
		for kv := range kvs {
			if count == 0 {
				word = string(kv.Key)
			}
			count += 1
		}
		out <- &kmrpb.KV{Key: []byte(word), Value: []byte(strconv.Itoa(count))}
		close(out)
	}()
	return out
}

func main() {
	cw := &executor.ComputeWrap{}
	cw.BindMapper(Map)
	cw.BindReducer(Reduce)
	cw.Run()
}
