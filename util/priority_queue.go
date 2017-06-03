package util

import (
	"container/heap"
)

// An Item is something we manage in a priority queue.
type Item struct {
	value interface{}
	src   int // source of data
	index int // The index of the item in the heap.
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue struct {
	items   []*Item
	compare func(a, b interface{}) int
}

func NewPriorityQueue(compareFunc func(a, b interface{}) int) *PriorityQueue {
	pq := &PriorityQueue{
		items:   make([]*Item, 0),
		compare: compareFunc,
	}
	heap.Init(pq)
	return pq
}

func (pq *PriorityQueue) Len() int { return len(pq.items) }

func (pq *PriorityQueue) Less(i, j int) bool {
	return pq.compare(pq.items[i], pq.items[j]) < 0
}

func (pq *PriorityQueue) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.items[i].index = i
	pq.items[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(pq.items)
	item := x.(*Item)
	item.index = n
	pq.items = append(pq.items, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := pq.items
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	pq.items = old[0 : n-1]
	return item
}

func (pq *PriorityQueue) Top() interface{} {
	return pq.items[0].value
}

func (pq *PriorityQueue) Enqueue(x interface{}, src int) {
	heap.Push(pq, &Item{value: x, src: src})
}

func (pq *PriorityQueue) Dequeue() (interface{}, int) {
	item := heap.Pop(pq).(*Item)
	return item.value, item.src
}
