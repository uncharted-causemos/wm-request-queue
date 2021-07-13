package queue

import (
	"container/list"
	"sync"
)

// ServiceRequestQueue is a baseline FIFO queue implementation that
type ServiceRequestQueue struct {
	queue  *list.List
	hashes map[int]bool
	size   int
	mutex  *sync.RWMutex
	cond   *sync.Cond
}

// NewServiceRequestQueue create a new ServiceRequestQueue that is immediately ready to
// receive enqueue requests.  The size of the queue is limited by the `size` parameter.
func NewServiceRequestQueue(size int) RequestQueue {
	mutex := &sync.RWMutex{}

	srQueue := &ServiceRequestQueue{
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
func (r *ServiceRequestQueue) Enqueue(x interface{}) bool {
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
func (r *ServiceRequestQueue) EnqueueHashed(key int, x interface{}) bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.queue.Len() < r.size {
		if !r.hashes[key] {
			r.queue.PushBack(&queuedItem{x: x, key: key})
			// signal that there's data available
			r.cond.Signal()
		}
		return true
	}
	return false
}

// Dequeue removes an item from the queue.  If the queue is empty, the operation blocks.
func (r *ServiceRequestQueue) Dequeue() interface{} {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// wait until there's data
	for r.queue.Len() == 0 {
		r.cond.Wait()
	}

	result := r.queue.Front()
	value := result.Value.(*queuedItem)
	r.queue.Remove(result)

	return value.x
}

// Size returns the curent size of the queue.
func (r *ServiceRequestQueue) Size() int {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	return r.queue.Len()
}
