package main

import (
	"io"
	"net"
	"strconv"
	"strings"
	"unicode"

	kmrpb "github.com/naturali/kmr/compute/pb"
	"github.com/naturali/kmr/util/log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	port = ":7782"
)

type wordCountSerer struct{}

func (s *wordCountSerer) ConfigMapper(stream kmrpb.Compute_ConfigMapperServer) error {
	return stream.SendAndClose(&kmrpb.ConfigResponse{Retcode: 0})
}

func (s *wordCountSerer) ConfigReducer(stream kmrpb.Compute_ConfigReducerServer) error {
	return stream.SendAndClose(&kmrpb.ConfigResponse{Retcode: 0})
}

func (s *wordCountSerer) Map(stream kmrpb.Compute_MapServer) error {
	for {
		kv, err := stream.Recv()
		// Flush all result
		if err == io.EOF {
			return nil
		}
		if err != nil {
			log.Error(err)
			return err
		}
		// split words
		for _, key := range strings.FieldsFunc(string(kv.Value), func(c rune) bool {
			return !unicode.IsLetter(c)
		}) {
			stream.Send(&kmrpb.KV{Key: []byte(key), Value: []byte(strconv.Itoa(1))})
		}
	}
}

func (s *wordCountSerer) Reduce(stream kmrpb.Compute_ReduceServer) error {
	word := ""
	count := 0
	for {
		kv, err := stream.Recv()
		if err == io.EOF {
			stream.Send(&kmrpb.KV{Key: []byte(word), Value: []byte(strconv.Itoa(count))})
			return nil
		}
		if err != nil {
			return err
		}
		if count == 0 {
			word = string(kv.Key)
		}
		count += 1
	}
}

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Debug("listening on port", port)
	s := grpc.NewServer()
	kmrpb.RegisterComputeServer(s, &wordCountSerer{})
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
