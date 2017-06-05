package records

import (
	"bytes"

	"github.com/naturali/kmr/util"
)

func MergeSort(readers []RecordReader, outputChan chan<- *Record) error {
	pq := util.NewPriorityQueue(func(a, b interface{}) int {
		x, y := a.(*Record), b.(*Record)
		return bytes.Compare(x.Key, y.Key)
	})
	for id, reader := range readers {
		if reader.HasNext() {
			pq.Enqueue(reader.Pop(), id)
		}
	}
	for pq.Len() > 0 {
		r, id := pq.Dequeue()
		outputChan <- r.(*Record)
		if readers[id].HasNext() {
			pq.Enqueue(readers[id].Pop(), id)
		}
	}
	close(outputChan)
	return nil
}
