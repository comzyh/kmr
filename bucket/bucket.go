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
func IntermediateFileName(mapID int, reduceID int, workerID int64) string {
	return fmt.Sprintf("%d-%d-%d.t", mapID, reduceID, workerID)
}

func FlushoutFileName(phase string, taskID int, flushID int, workerID int64) string {
	return fmt.Sprintf("flush-%s-%d-%d-%d.t", phase, taskID, flushID, workerID)
}
