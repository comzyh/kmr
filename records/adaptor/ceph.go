// +build linux
package records

import (
	"fmt"

	"github.com/ceph/go-ceph/rados"
)

type CephRecordReader struct {
	keys    []string
	preload chan Record
}

func NewCephRecordReader(key string) *CephRecordReader {
	args := []string{
		"--mon-host", "1.1.1.1",
	}
	conn, _ := rados.NewConn()
	err := conn.ParseCmdLineArgs(args)
	err = conn.Connect()
	ioctx, err := conn.OpenIOContext("ni")
	if err != nil {
		fmt.Println("OpenIOContext err: ", err)
	}

	preload := make(chan Record, 1000)
	go func() {
		buff := make([]byte, 5)
		var offset uint64 = 0
		for {
			n, err := ioctx.Read("obj", buff, offset)
			if err != nil {
				fmt.Printf("Cannot write %s, err: %v\n", "obj", err)
				break
			}
			if n > 0 {
				offset += uint64(n)
				// TODO: parse and push to preload
			} else {
				fmt.Printf("EOF")
				break
			}
			fmt.Printf("read count: %d, content: %s\n", n, buff[:n])
		}
		close(preload)
	}()

	return &CephRecordReader{
		key:     key,
		preload: preload,
	}
}

func (crr *CephRecordReader) Iter() <-chan Record {
	return crr.preload
}
