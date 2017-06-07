package main

import (
	"flag"
	"strings"

	"github.com/naturali/kmr/master"
)

var (
	port      = flag.String("port", ":50051", "port")
	jobName   = flag.String("jobname", "", "jobName")
	inputFile = flag.String("file", "", "input file path")
	dataDir   = flag.String("intermediate-dir", "/tmp/", "directory of intermediate files")
	nReduce   = flag.Int("nReduce", 1, "number of reducers")
)

func main() {
	flag.Parse()
	master.NewMapReduce(*port, *jobName, strings.Split(*inputFile, ","), *dataDir, *nReduce)
}
