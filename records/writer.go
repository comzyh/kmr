package records

import (
	"io"
	"os"
)

type RecordWriter interface {
	WriteRecord(Record) error
}

type SimpleRecordWriter struct {
	ioctx io.Writer
}

func (srw *SimpleRecordWriter) WriteRecord(record Record) error {
	return WriteRecord(srw.ioctx, record)
}

func NewConsoleRecordWriter() *SimpleRecordWriter {
	return &SimpleRecordWriter{
		ioctx: os.Stdout,
	}
}

func NewFileRecordWriter(filename string) *SimpleRecordWriter {
	// TODO:
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		panic("fail to create file reader")
	}

	return &SimpleRecordWriter{
		ioctx: file,
	}
}

func MakeRecordWriter(name string, params map[string]string) *SimpleRecordWriter {
	// TODO: registry
	// noway to instance directly by type name in Golang
	switch name {
	case "file":
		return NewFileRecordWriter(params["filename"])
	case "console":
		return NewConsoleRecordWriter()
	default:
		return NewConsoleRecordWriter()

	}
}
