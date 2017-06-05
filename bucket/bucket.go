package bucket

import (
	"fmt"

	"github.com/naturali/kmr/records"
)

// Bucket Object Pool to store objects
type Bucket interface {
	OpenRead(key string) (records.RecordReader, error)
	OpenWrite(key string) (records.RecordWriter, error)
}

// IntermediateFileName constructs the name of the intermediate file.
func IntermediateFileName(mapID int, reduceID int) string {
	return fmt.Sprintf("%d-%d.t", mapID, reduceID)
}
