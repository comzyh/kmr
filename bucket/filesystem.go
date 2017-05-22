package bucket

import (
	"io"
	"log"
	"os"
	"path/filepath"
)

// FSBucket use a directory as pool
type FSBucket struct {
	directory string
}

// NewFilePool NewFilePool
func NewFilePool(directory string) Bucket {
	return FSBucket{directory: directory}
}

func (fsb FSBucket) OpenRead(key string) (rd io.Reader, err error) {

	rd, err = os.OpenFile(filepath.Join(fsb.directory, key), os.O_RDONLY, 0666)
	if err != nil {
		log.Printf("Fail to open %v for read: %v", key, err)
	}
	return
}

func (fsb FSBucket) OpenWrite(key string) (wr io.Writer, err error) {
	wr, err = os.OpenFile(filepath.Join(fsb.directory, key), os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Printf("Fail to open %v for write: %v", key, err)
	}
	return
}
