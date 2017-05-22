package executor

import (
	"io"
	"log"
	"sort"

	"golang.org/x/net/context"

	kmrpb "github.com/naturali/kmr/compute/pb"
	"github.com/naturali/kmr/job"
	"github.com/naturali/kmr/records"
)

type Executor interface {
}

type ComputeWrap struct {
	Compute kmrpb.ComputeClient
}

type MapperWrap struct {
}

type ReducerWrap struct {
}

// ConvertRecortToKV
func ConvertRecordToKV(record *records.Record) *kmrpb.KV {
	return &kmrpb.KV{Key: record.Key, Value: record.Value}
}

// ConvertKVtToRecord
func ConvertKVtToRecord(kv *kmrpb.KV) *records.Record {
	return &records.Record{Key: kv.Key, Value: kv.Value}
}

// ConfigMapper Config a Mapper and deal with error
func (cw *ComputeWrap) ConfigMapper(kvs []kmrpb.KV) (reply *kmrpb.ConfigResponse, err error) {
	configMapperStram, err := cw.Compute.ConfigMapper(context.Background())
	if err != nil {
		log.Printf("%v.ConfigMapper(_) = _, %v\n", cw.Compute, err)
		return
	}
	for _, kv := range kvs {
		configMapperStram.Send(&kv)
	}
	reply, err = configMapperStram.CloseAndRecv()
	if err != nil {
		log.Printf("Failed to config Mapper, %v\n", err)
		return
	}
	if reply.Retcode != 0 {
		log.Printf("ConfigMapper retcode = %d\n", reply.Retcode)
	}
	return reply, nil
}

// ConfigReducer Config a Reducer and deal with error
func (cw *ComputeWrap) ConfigReducer(kvs []kmrpb.KV) (reply *kmrpb.ConfigResponse, err error) {
	configReducerStream, err := cw.Compute.ConfigReducer(context.Background())
	if err != nil {
		log.Printf("%v.ConfigReducer(_) = _, %v\n", cw.Compute, err)
		return
	}
	for _, kv := range kvs {
		configReducerStream.Send(&kv)
	}
	reply, err = configReducerStream.CloseAndRecv()
	if err != nil {
		log.Printf("Failed to config Mapper, %v\n", err)
		return
	}
	if reply.Retcode != 0 {
		log.Printf("ConfigReducer retcode = %d\n", reply.Retcode)
	}
	return reply, nil
}

// Map do Map operation and return aggregated
func (cw *ComputeWrap) Map(rr records.RecordReader) (aggregated []records.Record, err error) {
	mapStream, err := cw.Compute.Map(context.Background())
	if err != nil {
		log.Printf("%v.Map(_) = _, %v", cw.Compute, err)
		return
	}
	waitc := make(chan struct{})
	go func() {
		for {
			in, err := mapStream.Recv()
			if err == io.EOF {
				close(waitc)
				return
			}

			if err != nil {
				log.Printf("Failed to receive a note : %v", err)
				return
			}
			aggregated = append(aggregated, *ConvertKVtToRecord(in))
		}
	}()
	for rr.HasNext() {
		record := rr.Pop()
		mapStream.Send(ConvertRecordToKV(&record))
	}
	mapStream.CloseSend()
	<-waitc

	sort.Sort(records.ByKey(aggregated))
	// ctx.Write(aggregated)
	return
}

func (rw *ReducerWrap) Reduce(rr records.RecordReader, ctx job.Context) {
	// pre := records.Record{}
	// aggregated := []records.Record{}
	// rst := []records.Record{}
	//
	// for {
	// 	// FIXME: 64k
	// 	items, err := rr.ReadRecord(1 << 16)
	// 	if err != nil {
	// 	}
	// 	if len(items) == 0 {
	// 		break
	// 	}
	//
	// 	for item := range items {
	// 		if pre.Key == "" {
	// 			pre = item
	// 		} else {
	// 			if item.Key != pre.Key {
	// 				// grpc call compute with aggregated
	// 				reduceResult := []records.Record{}
	// 				rst = append(rst, reduceResult...)
	// 			} else {
	// 				aggregated = append(aggregated, item)
	// 			}
	// 		}
	//
	// 	}
	// }
	// ctx.Write(rst)
}
