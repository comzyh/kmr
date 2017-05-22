package main

import (
	"flag"
	"fmt"
	"io"
	"log"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	kmrpb "github.com/naturali/kmr/compute/pb"
)

// This is just a test tool for word count for now
func main() {
	log.Println("executor started.")

	computeAddress := flag.String("compute", "localhost:7782", "ip:port for coumpte instance")

	conn, err := grpc.Dial(*computeAddress, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Can  not connect to Compute instance %s: %v\n", *computeAddress, err)
	}
	defer conn.Close()
	compute := kmrpb.NewComputeClient(conn)

	// Mapper
	configMapStream, err := compute.ConfigMapper(context.Background())
	if err != nil {
		log.Fatalf("%v.ConfigMapper(_) = _, %v", compute, err)
	}
	reply, err := configMapStream.CloseAndRecv()
	if err != nil {
		log.Fatalf("Failed to config Mapper, %v\n", err)
	}
	fmt.Printf("reply: %v\n", reply.Retcode)
	mapStream, err := compute.Map(context.Background())
	if err != nil {
		log.Fatalf("%v.Map(_) = _, %v", compute, err)
	}
	mapStream.Send(&kmrpb.KV{Key: []byte(""), Value: []byte("This eBook is for the use of anyone anywhere at no cost and with")})
	mapStream.CloseSend()
	for {
		in, err := mapStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Failed to receive a Map result : %v", err)
		}
		fmt.Println(string(in.Key), ":", string(in.Value))
	}

	log.Println("Exit executor")
}
