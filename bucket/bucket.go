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

// NewBucket Bucket factory
func NewBucket(bucketType string, config map[string]interface{}) (Bucket, error) {
	switch bucketType {
	case "filesystem":
		directory, ok := config["directory"]
		if !ok {
			return nil, fmt.Errorf("directory is not provided")
		}

		return NewFSBucket(directory.(string))
	case "rados":
		mons, ok := config["mons"]
		if !ok {
			return nil, fmt.Errorf("mons is not provided")
		}
		secret, ok := config["secret"]
		if !ok {
			return nil, fmt.Errorf("secret is not provided")
		}
		pool, ok := config["pool"]
		if !ok {
			return nil, fmt.Errorf("pool is not provided")
		}
		prefix, ok := config["prefix"]
		if !ok {
			prefix = ""
		}
		return NewRadosBucket(mons.(string), secret.(string), pool.(string), prefix.(string))
	case "azureblob":
		accountName, ok := config["accountName"]
		if !ok {
			return nil, fmt.Errorf("accountName is not provided")
		}
		accountKey, ok := config["accountKey"]
		if !ok {
			return nil, fmt.Errorf("accountKey is not provided")
		}
		containerName, ok := config["containerName"]
		if !ok {
			return nil, fmt.Errorf("containerName is not provided")
		}
		blobServiceBaseUrl, ok := config["blobServiceBaseUrl"]
		if !ok {
			return nil, fmt.Errorf("blobServiceBaseUrl is not provided")
		}
		apiVersion, ok := config["apiVersion"]
		if !ok {
			return nil, fmt.Errorf("apiVersion is not provided")
		}
		useHttps, ok := config["useHttps"]
		if !ok {
			return nil, fmt.Errorf("useHttps is not provided")
		}
		directory, ok := config["directory"]
		if !ok {
			return nil, fmt.Errorf("directory is not provided")
		}
		return NewAzureBlobBucket(accountName.(string), accountKey.(string), containerName.(string),
			blobServiceBaseUrl.(string), apiVersion.(string), useHttps.(bool), directory.(string))
	default:
		return nil, fmt.Errorf("Unknown bucket type \"%s\"", bucketType)
	}
}

// IntermediateFileName constructs the name of the intermediate file.
func IntermediateFileName(mapID int, reduceID int, workerID int64) string {
	return fmt.Sprintf("%d-%d-%d.t", mapID, reduceID, workerID)
}

// FlushoutFileName construct the name of flushout file
func FlushoutFileName(phase string, taskID int, flushID int, workerID int64) string {
	return fmt.Sprintf("flush-%s-%d-%d-%d.t", phase, taskID, flushID, workerID)
}
