package records

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

type RecordReader interface {
	Peek() Record // get first record without drop it
	Pop()
	HasNext() bool
}

type SimpleRecordReader struct {
	input <-chan Record
	first *Record
}

func NewSimpleRecordReader(input <-chan Record) *SimpleRecordReader {
	reader := SimpleRecordReader{
		input: input,
		first: nil,
	}
	return &reader
}

func (srr *SimpleRecordReader) Peek() Record {
	return *srr.first
}

func (srr *SimpleRecordReader) Pop() {
	srr.first = nil
}

func (srr *SimpleRecordReader) HasNext() bool {
	if srr.first != nil {
		return true
	}
	record, ok := <-srr.input
	if !ok {
		return false
	}
	srr.first = &record
	return true
}

func NewConsoleRecordReader() *SimpleRecordReader {
	reader := bufio.NewReader(os.Stdin)
	preload := make(chan Record, 1000)

	feedStream(preload, reader)

	return &SimpleRecordReader{
		input: preload,
	}
}

func NewFileRecordReader(filename string) *SimpleRecordReader {
	file, err := os.Open(filename)
	if err != nil {
		panic("fail to create file reader")
	}
	reader := bufio.NewReader(file)
	preload := make(chan Record, 1000)

	feedStream(preload, reader)

	return &SimpleRecordReader{
		input: preload,
	}
}

func feedStream(preload chan<- Record, reader io.Reader) {
	go func() {
		for {
			var err error
			// Read Key
			record, err := ReadRecord(reader)
			if err == io.EOF {
				break
			}
			preload <- record
		}
		close(preload)
	}()
}

// feedTextStream read text file, emit (linenumber.(string), line.([]byte))
func feedTextStream(preload chan<- Record, reader io.Reader) {
	go func() {
		r := bufio.NewReader(reader)
		var lineNum int32
		for line, _, err := r.ReadLine(); err != io.EOF; { // TODO: deal with isPerfix
			preload <- Record{Key: []byte(fmt.Sprint(lineNum)), Value: line}
			lineNum++
		}
		close(preload)
	}()
}

func MakeRecordReader(name string, params map[string]string) RecordReader {
	// TODO: registry
	// noway to instance directly by type name in Golang
	switch name {
	case "file":
		return NewFileRecordReader(params["filename"])
	case "console":
		return NewConsoleRecordReader()
	default:
		return NewConsoleRecordReader()

	}
}
