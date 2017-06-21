package bucket

import (
	"bufio"
	"log"
	"os"
	"path/filepath"

	"github.com/naturali/kmr/records"
)

// FSBucket use a directory as pool
type FSBucket struct {
	directory string
}

// FileRecordReader FileRecordReader
type FileRecordReader struct {
	file   *os.File
	reader *bufio.Reader
	srr    *records.SimpleRecordReader
}

func (reader FileRecordReader) Peek() *records.Record {
	return reader.srr.Peek()
}

func (reader FileRecordReader) HasNext() bool {
	return reader.srr.HasNext()
}

func (reader FileRecordReader) Pop() *records.Record {
	return reader.srr.Pop()
}

func (reader FileRecordReader) Close() error {
	return reader.file.Close()
}

// FileRecordWriter FileRecordWriter
type FileRecordWriter struct {
	// records.RecordWriter
	file   *os.File
	writer *bufio.Writer
}

func (writer FileRecordWriter) Close() error {
	err := writer.Flush()
	if err != nil {
		return err
	}
	return writer.file.Close()
}

func (writer FileRecordWriter) Flush() error {
	return writer.writer.Flush()
}

func (writer FileRecordWriter) Write(data []byte) (int, error) {
	return writer.writer.Write(data)
}

func (writer FileRecordWriter) WriteRecord(record *records.Record) error {
	return records.WriteRecord(writer, record)
}

// NewFilePool NewFilePool
func NewFilePool(directory string) (bk Bucket, err error) {
	if _, err = os.Stat(directory); os.IsNotExist(err) {
		err = os.MkdirAll(directory, 0755)
		if err != nil {
			return
		}
	}
	return FSBucket{directory: directory}, nil
}

// OpenRead Open a RecordReader by name
func (fsb FSBucket) OpenRead(key string) (rd records.RecordReader, err error) {
	var reader FileRecordReader
	reader.file, err = os.OpenFile(filepath.Join(fsb.directory, key), os.O_RDONLY, 0666)
	if err != nil {
		log.Printf("Fail to open %v for read: %v", key, err)
	}
	reader.reader = bufio.NewReader(reader.file)
	reader.srr = records.NewStreamRecordReader(reader.reader)
	return reader, nil
}

// OpenWrite Open a RecordWriter by name
func (fsb FSBucket) OpenWrite(key string) (wr records.RecordWriter, err error) {
	var writer FileRecordWriter
	writer.file, err = os.OpenFile(filepath.Join(fsb.directory, key), os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Printf("Fail to open %v for write: %v", key, err)
	}
	writer.writer = bufio.NewWriter(writer.file)
	return writer, nil
}
