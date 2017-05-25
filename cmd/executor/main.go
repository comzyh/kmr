package main

import (
	"flag"
	"fmt"
	"log"

	"google.golang.org/grpc"

	"github.com/naturali/kmr/bucket"
	kmrpb "github.com/naturali/kmr/compute/pb"
	"github.com/naturali/kmr/executor"
	"github.com/naturali/kmr/records"
)

// This is just a test tool for word count for now
func main() {
	log.Println("executor started.")

	computeAddress := flag.String("compute", "localhost:7782", "ip:port for coumpte instance")
	inputFile := flag.String("file", "", "input file path")
	flag.Parse()

	conn, err := grpc.Dial(*computeAddress, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Can  not connect to Compute instance %s: %v\n", *computeAddress, err)
	}
	defer conn.Close()
	compute := executor.ComputeWrap{Compute: kmrpb.NewComputeClient(conn)}
	//ConfigMapper
	reply, err := compute.ConfigMapper(nil)
	if err != nil || reply.Retcode != 0 {
		log.Fatalf("Fail to config mapper: %v", err)
	}
	// Mapper
	rr := records.MakeRecordReader("textfile", map[string]interface{}{"filename": *inputFile})
	aggregated, err := compute.Map(rr)
	if err != nil {
		log.Fatalf("Fail to Map: %v", err)
	}
	bk, err := bucket.NewFilePool("/tmp/xsdx")
	if err != nil {
		log.Fatalf("Fail to open bucket: %v", err)
	}
	writer, err := bk.OpenWrite("intermediate.dat")
	if err != nil {
		log.Fatalf("Failed to open intermediate: %v", err)
	}
	rw := records.MakeRecordWriter("stream", map[string]interface{}{"writer": writer})
	for _, record := range aggregated {
		rw.WriteRecord(record)
		fmt.Println(string(record.Key), ":", string(record.Value))
	}
	log.Println("Exit executor")
}
