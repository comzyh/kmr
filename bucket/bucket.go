package bucket

import "io"

// Bucket Object Pool to store objects
type Bucket interface {
	OpenRead(key string) (io.Reader, error)
	OpenWrite(key string) (io.Writer, error)
}
