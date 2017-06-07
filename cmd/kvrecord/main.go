package main

import (
	"fmt"
	"os"

	"github.com/naturali/kmr/records"
)

func main() {
	if len(os.Args) == 2 {
		rr := records.MakeRecordReader("file", map[string]interface{}{"filename": os.Args[1]})
		for rr.HasNext() {
			r := rr.Pop()
			fmt.Println(string(r.Key), string(r.Value))
		}
	} else {
		fmt.Println("Usage: kvrecord <filename>")
	}
}
