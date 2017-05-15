package main

import (
	"log"
	"net"

	kmrpb "github.com/naturali/kmr/compute/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	port = ":7782"
)

type wordCountSerer struct{}

func (s *wordCountSerer) ConfigMapper(stream kmrpb.Compute_ConfigMapperServer) error {
	return nil
}

func (s *wordCountSerer) ConfigReducer(stream kmrpb.Compute_ConfigReducerServer) error {
	return nil
}

func (s *wordCountSerer) Map(stream kmrpb.Compute_MapServer) error {
	return nil
}

func (s *wordCountSerer) Reduce(stream kmrpb.Compute_ReduceServer) error {
	return nil
}

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	kmrpb.RegisterComputeServer(s, &wordCountSerer{})
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
