package executor

import (
	"bytes"
	"flag"
	"sort"
	"strconv"
	"sync"
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
	postFunc   func(kvs <-chan *kmrpb.KV) <-chan struct{}
}

func (cw *ComputeWrap) BindMapper(mapper func(kvs <-chan *kmrpb.KV) <-chan *kmrpb.KV) {
	cw.mapFunc = mapper
}

func (cw *ComputeWrap) BindReducer(reducer func(kvs <-chan *kmrpb.KV) <-chan *kmrpb.KV) {
	cw.reduceFunc = reducer
}

func (cw *ComputeWrap) BindPostFunction(post func(kvs <-chan *kmrpb.KV) <-chan struct{}) {
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
	switch *phase {
	case "map":
		log.Infof("starting id%d mapper", *mapID)

		rr := records.MakeRecordReader("textfile", map[string]interface{}{"filename": *inputFile})
		bk, err := bucket.NewFilePool(*dataDir + "/" + *jobName)
		if err != nil {
			log.Fatalf("Fail to open bucket: %v", err)
		}
		// Mapper
		if err := cw.doMap(rr, bk, *mapID, *nReduce); err != nil {
			log.Fatalf("Fail to Map: %v", err)
		}
	case "reduce":
		log.Infof("starting id%d reducer", *reduceID)

		bk, err := bucket.NewFilePool(*dataDir + "/" + *jobName)
		if err != nil {
			log.Fatalf("Fail to open bucket: %v", err)
		}
		// Reduce
		res, err := cw.doReduce(bk, *reduceID, *nMap)
		if err != nil {
			log.Fatalf("Fail to Reduce: %v", err)
		}
		outputFile := *dataDir + "/" + *jobName + "/" + "res-" + strconv.Itoa(*reduceID) + ".t"
		rw := records.MakeRecordWriter("file", map[string]interface{}{"filename": outputFile})
		for _, r := range res {
			rw.WriteRecord(KVToRecord(r))
		}

		if cw.postFunc != nil {
			inputKV := make(chan *kmrpb.KV, 1024)
			waitc := cw.postFunc(inputKV)
			for _, r := range res {
				inputKV <- r
			}
			close(inputKV)
			<-waitc
		}
	default:
		panic("unsupported phase")
	}
	log.Info("Exit executor")
}

// doMap does map operation and save the intermediate files.
func (cw *ComputeWrap) doMap(rr records.RecordReader, bk bucket.Bucket, mapID int, nReduce int) (err error) {
	startTime := time.Now()
	aggregated := make([][]*records.Record, 0)
	for i := 0; i < nReduce; i++ {
		aggregated = append(aggregated, make([]*records.Record, 0))
	}

	// map
	waitc := make(chan struct{})
	inputKV := make(chan *kmrpb.KV, 1024)
	outputKV := cw.mapFunc(inputKV)
	go func() {
		for in := range outputKV {
			rBucketID := util.HashBytesKey(in.Key) % nReduce
			aggregated[rBucketID] = append(aggregated[rBucketID], KVToRecord(in))
		}
		close(waitc)
	}()
	for rr.HasNext() {
		inputKV <- RecordToKV(rr.Pop())
	}
	close(inputKV)
	<-waitc
	log.Debug("DONE Map. Took:", time.Since(startTime))

	// write intermediate files
	var wg sync.WaitGroup
	for i := 0; i < nReduce; i++ {
		wg.Add(1)
		go func(reduceID int) {
			sort.Sort(records.ByKey(aggregated[reduceID]))
			intermediateFileName := bucket.IntermediateFileName(mapID, reduceID)
			writer, err := bk.OpenWrite(intermediateFileName)
			if err != nil {
				log.Fatalf("Failed to open intermediate: %v", err)
			}
			for _, record := range aggregated[reduceID] {
				writer.WriteRecord(record)
			}
			writer.Close()
			wg.Done()
			log.Debug("DONE Write", intermediateFileName)
		}(i)
	}
	wg.Wait()

	log.Debug("FINISH Write IntermediateFiles. Took:", time.Since(startTime))
	return
}

// doReduce does reduce operation
func (cw *ComputeWrap) doReduce(bk bucket.Bucket, reduceID int, nMap int) ([]*kmrpb.KV, error) {
	readers := make([]records.RecordReader, 0)
	for i := 0; i < nMap; i++ {
		reader, err := bk.OpenRead(bucket.IntermediateFileName(i, reduceID))
		if err != nil {
			log.Fatalf("Failed to open intermediate: %v", err)
		}
		readers = append(readers, reader)
	}
	outputRecords := make([]*kmrpb.KV, 0)
	sorted := make(chan *records.Record, 1024)
	go records.MergeSort(readers, sorted)
	var lastKey []byte
	values := make([][]byte, 0)
	for r := range sorted {
		if bytes.Equal(lastKey, r.Key) {
			values = append(values, r.Value)
			continue
		}
		if lastKey != nil {
			res, _ := cw.doReduceForSingleKey(lastKey, values)
			outputRecords = append(outputRecords, res...)
		}
		values = values[:0]
		lastKey = r.Key
		values = append(values, r.Key)
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

// RecordToKV converts an Record to a kmrpb.KV
func RecordToKV(record *records.Record) *kmrpb.KV {
	return &kmrpb.KV{Key: record.Key, Value: record.Value}
}

// KVToRecord converts a kmrpb.KV to an Record
func KVToRecord(kv *kmrpb.KV) *records.Record {
	return &records.Record{Key: kv.Key, Value: kv.Value}
}
