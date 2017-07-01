// +build linux
package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/naturali/kmr/bucket"
)

var (
	mons   = flag.String("mons", "", "ceph moniters")
	secret = flag.String("secret", "", "ceph auth secret")
	pool   = flag.String("pool", "", "ceph rados pool")
	prefix = flag.String("prefix", "/tmp/", "object prefix for ceph objectname, like '/job/'")
)

func main() {
	flag.Parse()
	bk, err := bucket.NewRadosBucket(*mons, *secret, *pool, *prefix)
	if err != nil {
		log.Fatalf("Can't connnect ceph pool: %v ", err)
	}
	objName := "test-object"

	// Test write
	writer, err := bk.OpenWrite(objName)
	if err != nil {
		log.Fatalf("Can't open %s for write: %v", objName, err)
	}
	lines := []string{
		"Hello World\n",
		"This is the firsst Line,\n",
		"And this is the second line",
		fmt.Sprintf("now is: %v", time.Now()),
	}

	for _, text := range lines {
		n, err := writer.Write([]byte(text))
		fmt.Println(n, err)
	}
	writer.Close()

	// Test read
	reader, err := bk.OpenRead(objName)
	if err != nil {
		log.Fatalf("Can't open %s for read: %v", objName, err)
	}
	buffer := make([]byte, 7)
	for {
		n, err := reader.Read(buffer)
		if err != nil {
			fmt.Println("n: ", n, "err:", err)
			break
		}
		fmt.Println(n, buffer)
	}
	reader.Close()

	err = bk.Delete(objName)
	if err != nil {
		log.Fatalf("Can't elete %s: %v", objName, err)
	}
}
