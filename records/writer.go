package records

import (
	"io"
	"os"
)

type RecordWriter interface {
	WriteRecord(Record) error
	Close() error
}

type SimpleRecordWriter struct {
	writer io.Writer
}

func (srw *SimpleRecordWriter) WriteRecord(record Record) error {
	return WriteRecord(srw.writer, record)
}

func NewConsoleRecordWriter() *SimpleRecordWriter {
	return &SimpleRecordWriter{
		writer: os.Stdout,
	}
}

func NewFileRecordWriter(filename string) *SimpleRecordWriter {
	// TODO:
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		panic("fail to create file reader")
	}

	return &SimpleRecordWriter{
		writer: file,
	}
}
func NewStreamRecordWriter(writer io.Writer) *SimpleRecordWriter {
	return &SimpleRecordWriter{
		writer: writer,
	}
}

func MakeRecordWriter(name string, params map[string]interface{}) *SimpleRecordWriter {
	// TODO: registry
	// noway to instance directly by type name in Golang
	switch name {
	case "file":
		return NewFileRecordWriter(params["filename"].(string))
	case "console":
		return NewConsoleRecordWriter()
	case "stream":
		return NewStreamRecordWriter(params["writer"].(io.Writer))
	default:
		return NewConsoleRecordWriter()

	}
}
