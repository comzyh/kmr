package bucket

import (
	"github.com/naturali/kmr/records"
	"strconv"
)

// Bucket Object Pool to store objects
type Bucket interface {
	OpenRead(key string) (records.RecordReader, error)
	OpenWrite(key string) (records.RecordWriter, error)
}

func IntermidateFileName(mapID int, reduceID int) string {
	return strconv.Itoa(mapID) + "-" + strconv.Itoa(reduceID) + ".t"
}
