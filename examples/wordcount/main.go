package main

import (
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"unicode"

	kmrpb "github.com/naturali/kmr/compute/pb"
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
	wordMap := make(map[string]int)
	for {
		kv, err := stream.Recv()
		// Flush all result
		if err == io.EOF {
			for key, value := range wordMap {
				stream.Send(&kmrpb.KV{Key: []byte(key), Value: []byte(strconv.Itoa(value))})
			}
			return nil
		}
		if err != nil {
			return err
		}
		// split words
		for _, key := range strings.FieldsFunc(string(kv.Value), func(c rune) bool {
			return !unicode.IsLetter(c)
		}) {
			wordMap[key]++
		}
	}
}

func (s *wordCountSerer) Reduce(stream kmrpb.Compute_ReduceServer) error {
	word := ""
	count := 0
	emit := func() {
		stream.Send(&kmrpb.KV{Key: []byte(word), Value: []byte(strconv.Itoa(count))})
	}
	for {
		kv, err := stream.Recv()
		if err == io.EOF {
			emit()
			return nil
		}
		if err != nil {
			return err
		}
		if string(kv.Key) == word {
			c, _ := strconv.Atoi(string(kv.Value))
			count += c
		} else {
			emit()
			word = string(kv.Key)
			count, _ = strconv.Atoi(string(kv.Value))
		}
	}
}

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Println("listening on port", port)
	s := grpc.NewServer()
	kmrpb.RegisterComputeServer(s, &wordCountSerer{})
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
