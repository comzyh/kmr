package executor

import (
	"bytes"
	"flag"
	"sort"
	"strconv"
	"time"

	"github.com/naturali/kmr/bucket"
	kmrpb "github.com/naturali/kmr/compute/pb"
	"github.com/naturali/kmr/records"
	"github.com/naturali/kmr/util"
	"github.com/naturali/kmr/util/log"
)

type ComputeWrap struct {
	mapFunc    func(kvs <-chan *kmrpb.KV) <-chan *kmrpb.KV
	reduceFunc func(kvs <-chan *kmrpb.KV) <-chan *kmrpb.KV
	postFunc   func(kvs <-chan *kmrpb.KV)
}

func (cw *ComputeWrap) BindMapper(mapper func(kvs <-chan *kmrpb.KV) <-chan *kmrpb.KV) {
	cw.mapFunc = mapper
}

func (cw *ComputeWrap) BindReducer(reducer func(kvs <-chan *kmrpb.KV) <-chan *kmrpb.KV) {
	cw.reduceFunc = reducer
}

func (cw *ComputeWrap) BindPostFunction(post func(kvs <-chan *kmrpb.KV)) {
	cw.postFunc = post
}

func (cw *ComputeWrap) Run() {
	var (
		jobName   = flag.String("jobname", "wc", "jobName")
		inputFile = flag.String("file", "", "input file path")
		dataDir   = flag.String("intermediate-dir", "/tmp/", "directory of intermediate files")
		phase     = flag.String("phase", "", "map or reduce")
		nMap      = flag.Int("nMap", 1, "number of mappers")
		nReduce   = flag.Int("nReduce", 1, "number of reducers")
		mapID     = flag.Int("mapID", 0, "mapper id")
		reduceID  = flag.Int("reduceID", 0, "reducer id")
	)
	flag.Parse()
	if *phase == "map" {
		log.Infof("starting id%d mapper", *mapID)

		//ConfigMapper
		rr := records.MakeRecordReader("textfile", map[string]interface{}{"filename": *inputFile})
		bk, err := bucket.NewFilePool(*dataDir + "/" + *jobName)
		if err != nil {
			log.Fatalf("Fail to open bucket: %v", err)
		}
		// Mapper
		if err := cw.doMap(rr, bk, *mapID, *nReduce); err != nil {
			log.Fatalf("Fail to Map: %v", err)
		}
	} else if *phase == "reduce" {
		log.Infof("starting id%d reducer", *reduceID)
		_ = nMap

		//ConfigReducer
		//
		bk, err := bucket.NewFilePool(*dataDir + "/" + *jobName)
		if err != nil {
			log.Fatalf("Fail to open bucket: %v", err)
		}
		// Reduce
		if res, err := cw.doReduce(bk, *reduceID, *nMap); err != nil {
			log.Fatalf("Fail to Map: %v", err)
		} else {
			outputFile := *dataDir + "/" + *jobName + "/" + "res-" + strconv.Itoa(*reduceID) + ".t"
			rw := records.MakeRecordWriter("file", map[string]interface{}{"filename": outputFile})
			for _, r := range res {
				rw.WriteRecord(ConvertKVtToRecord(r))
			}

			if cw.postFunc != nil {
				inputKV := make(chan *kmrpb.KV, 1024)
				cw.postFunc(inputKV)
				for _, r := range res {
					inputKV <- r
				}
			}
		}
	}
	log.Info("Exit executor")
}

// Map do Map operation and return aggregated
func (cw *ComputeWrap) doMap(rr records.RecordReader, bk bucket.Bucket, mapID int, nReduce int) (err error) {
	startTime := time.Now()
	aggregated := make([][]*records.Record, 0)
	for i := 0; i < nReduce; i++ {
		aggregated = append(aggregated, make([]*records.Record, 0))
	}
	waitc := make(chan struct{})
	inputKV := make(chan *kmrpb.KV, 1024)
	outputKV := cw.mapFunc(inputKV)

	go func() {
		for in := range outputKV {
			rBucketID := util.HashBytesKey(in.Key) % nReduce
			aggregated[rBucketID] = append(aggregated[rBucketID], ConvertKVtToRecord(in))
		}
		close(waitc)
	}()

	for rr.HasNext() {
		inputKV <- ConvertRecordToKV(rr.Pop())
	}
	close(inputKV)
	<-waitc

	log.Debug("DONE Map", time.Now().Sub(startTime))

	for i := 0; i < nReduce; i++ {
		sort.Sort(records.ByKey(aggregated[i]))
		log.Debug("DONE Map", time.Now().Sub(startTime))
		writer, err := bk.OpenWrite(bucket.IntermidateFileName(mapID, i))
		if err != nil {
			log.Fatalf("Failed to open intermediate: %v", err)
		}
		rw := records.MakeRecordWriter("stream", map[string]interface{}{"writer": writer})
		for _, record := range aggregated[i] {
			rw.WriteRecord(record)
		}
	}

	log.Debug("FINISH Write", time.Now().Sub(startTime))
	return
}

func (cw *ComputeWrap) doReduce(bk bucket.Bucket, reduceID int, nMap int) ([]*kmrpb.KV, error) {
	readers := make([]records.RecordReader, 0)
	for i := 0; i < nMap; i++ {
		reader, err := bk.OpenRead(bucket.IntermidateFileName(i, reduceID))
		if err != nil {
			log.Fatalf("Failed to open intermediate: %v", err)
		}
		readers = append(readers, records.MakeRecordReader("stream", map[string]interface{}{"reader": reader}))
	}
	outputRecords := make([]*kmrpb.KV, 0)
	inputs := make(chan *records.Record, 1024)
	go records.MergeSort(readers, inputs)
	var lastKey []byte
	values := make([][]byte, 0)
	for r := range inputs {
		if lastKey == nil || bytes.Compare(lastKey, r.Key) != 0 {
			if lastKey != nil {
				res, _ := cw.doReduceForSingleKey(lastKey, values)
				outputRecords = append(outputRecords, res...)
			}
			values = values[:0]
			lastKey = r.Key
		}
		values = append(values, r.Value)
	}
	if lastKey != nil {
		res, _ := cw.doReduceForSingleKey(lastKey, values)
		outputRecords = append(outputRecords, res...)
	}
	return outputRecords, nil
}

func (cw *ComputeWrap) doReduceForSingleKey(key []byte, values [][]byte) ([]*kmrpb.KV, error) {
	waitc := make(chan struct{})
	inputKV := make(chan *kmrpb.KV, 1024)
	outputKV := cw.reduceFunc(inputKV)
	ret := make([]*kmrpb.KV, 0)
	go func() {
		for in := range outputKV {
			ret = append(ret, in)
		}
		close(waitc)
	}()
	for _, v := range values {
		inputKV <- &kmrpb.KV{
			Key:   key,
			Value: v,
		}
	}
	close(inputKV)
	<-waitc
	return ret, nil
}

// ConvertRecortToKV
func ConvertRecordToKV(record *records.Record) *kmrpb.KV {
	return &kmrpb.KV{Key: record.Key, Value: record.Value}
}

// ConvertKVtToRecord
func ConvertKVtToRecord(kv *kmrpb.KV) *records.Record {
	return &records.Record{Key: kv.Key, Value: kv.Value}
}
