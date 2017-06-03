package bucket

import "github.com/naturali/kmr/records"

// Bucket Object Pool to store objects
type Bucket interface {
	OpenRead(key string) (records.RecordReader, error)
	OpenWrite(key string) (records.RecordWriter, error)
}
