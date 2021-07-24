package queue

import (
	"container/list"
	"sync"
)

// RequestQueue defines an interface for a request queue that supports enqueuing and dequeuing operations.
type RequestQueue interface {
	Enqueue(x interface{}) bool
	EnqueueHashed(key int, x interface{}) bool
	Dequeue() interface{}
	Clear()
	Size() int
}

type queuedItem struct {
	key int
	x   interface{}
}

// ListFIFOQueue is a FIFO queue implementation based on a doubly linked list.
type ListFIFOQueue struct {
	queue  *list.List
	hashes map[int]bool
	size   int
	mutex  *sync.RWMutex
	cond   *sync.Cond
}

// NewListFIFOQueue create a new ServiceRequestQueue that is immediately ready to
// receive enqueue requests.  The size of the queue is limited by the `size` parameter.
func NewListFIFOQueue(size int) RequestQueue {
	mutex := &sync.RWMutex{}

	srQueue := &ListFIFOQueue{
		queue:  list.New(),
		hashes: map[int]bool{},
		size:   size,
		mutex:  mutex,
		cond:   sync.NewCond(mutex),
	}

	return srQueue
}

// Enqueue adds a new item to the queue.  If the queue is full, the item will not
// be added, and the function will return `false`.
func (r *ListFIFOQueue) Enqueue(x interface{}) bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.queue.Len() < r.size {
		// add data
		r.queue.PushBack(&queuedItem{x: x})
		r.cond.Signal()
		return true
	}
	return false
}

// EnqueueHashed adds a new item to the queue if an item with a similar hash doesn't already exist.
// If the queue is full, the item will not be added, and the function will return `false`.  If an
func (r *ListFIFOQueue) EnqueueHashed(key int, x interface{}) bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.queue.Len() < r.size {
		if !r.hashes[key] {
			r.queue.PushBack(&queuedItem{x: x, key: key})
			r.hashes[key] = true
			// signal that there's data available
			r.cond.Signal()
		}
		return true
	}
	return false
}

// Dequeue removes an item from the queue.  If the queue is empty, the operation blocks.
func (r *ListFIFOQueue) Dequeue() interface{} {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// wait until there's data
	for r.queue.Len() == 0 {
		r.cond.Wait()
	}

	// peek at the data
	result := r.queue.Front()
	value := result.Value.(*queuedItem)

	// remove the item from the queue and the hash set if necessary
	r.queue.Remove(result)
	if r.hashes[value.key] {
		delete(r.hashes, value.key)
	}

	return value.x
}

// Size returns the curent size of the queue.
func (r *ListFIFOQueue) Size() int {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	return r.queue.Len()
}

// Clear clears the queue and request key hash map.
func (r *ListFIFOQueue) Clear() {
	r.mutex.Lock()
	r.queue.Init()
	r.hashes = map[int]bool{}
	r.mutex.Unlock()
}