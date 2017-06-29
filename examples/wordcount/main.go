package main

import (
	"bytes"
	"encoding/binary"
	"strings"
	"unicode"

	"github.com/naturali/kmr/executor"
	kmrpb "github.com/naturali/kmr/pb"
)

const (
	MAX_WORD_LENGTH = 20
)

func isAlphaOrNumber(r rune) bool {
	return 'a' <= r && r <= 'z' || 'A' <= r && r <= 'Z' || unicode.IsDigit(r)
}

func isChinese(r rune) bool {
	return r >= '\u4e00' && r <= '\u9fa5'
}

func ProcessSingleSentence(line string) []string {
	outputs := make([]string, 0)
	e_word := ""
	for _, r := range line {
		if isChinese(r) {
			if len(e_word) > 0 {
				outputs = append(outputs, e_word)
				e_word = ""
			}
			outputs = append(outputs, string(r))
		} else if isAlphaOrNumber(r) {
			e_word += string(r)
		} else {
			if len(e_word) > 0 {
				outputs = append(outputs, e_word)
				e_word = ""
			}
		}
	}
	if len(e_word) > 0 {
		outputs = append(outputs, e_word)
	}
	return outputs
}

func Map(kvs <-chan *kmrpb.KV) <-chan *kmrpb.KV {
	out := make(chan *kmrpb.KV, 1024)
	go func() {
		b := make([]byte, 8)
		for kv := range kvs {
			for _, procceed := range ProcessSingleSentence(strings.Trim(string(kv.Value), "\n")) {
				if len(procceed) > MAX_WORD_LENGTH {
					continue
				}
				binary.LittleEndian.PutUint64(b, uint64(1))
				out <- &kmrpb.KV{Key: []byte(procceed), Value: b}
			}
		}
		close(out)
	}()
	return out
}

func Reduce(kvs <-chan *kmrpb.KV) <-chan *kmrpb.KV {
	out := make(chan *kmrpb.KV, 1024)
	go func() {
		b := make([]byte, 8)
		var key []byte
		var count uint64
		for kv := range kvs {
			if !bytes.Equal(key, kv.Key) {
				if key != nil {
					binary.LittleEndian.PutUint64(b, count)
					out <- &kmrpb.KV{Key: key, Value: b}
				}
				key = kv.Key
				count = 0
			}
			count += binary.LittleEndian.Uint64(kv.Value)
		}
		if key != nil {
			binary.LittleEndian.PutUint64(b, count)
			out <- &kmrpb.KV{Key: key, Value: b}
		}
		close(out)
	}()
	return out
}

func Combine(v1 []byte, v2 []byte) []byte {
	var count uint64
	count += binary.LittleEndian.Uint64(v1)
	count += binary.LittleEndian.Uint64(v2)
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, count)
	return b
}

func main() {
	cw := &executor.ComputeWrap{}
	cw.BindMapper(Map)
	cw.BindReducer(Reduce)
	cw.BindCombiner(Combine)
	cw.Run()
}
