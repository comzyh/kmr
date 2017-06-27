package bucket

import (
	"fmt"
	"io"
)

// ObjectReader ObjectReader
type ObjectReader interface {
	io.Reader
	Close() error
}

// ObjectWriter ObjectWriter
type ObjectWriter interface {
	io.Writer
	Close() error
}

// Bucket Object Pool to store objects
type Bucket interface {
	OpenRead(key string) (ObjectReader, error)
	OpenWrite(key string) (ObjectWriter, error)
	Delete(key string) error
}

// IntermediateFileName constructs the name of the intermediate file.
func IntermediateFileName(mapID int, reduceID int, workerID int64) string {
	return fmt.Sprintf("%d-%d-%d.t", mapID, reduceID, workerID)
}

// FlushoutFileName construct the name of flushout file
func FlushoutFileName(phase string, taskID int, flushID int, workerID int64) string {
	return fmt.Sprintf("flush-%s-%d-%d-%d.t", phase, taskID, flushID, workerID)
}
