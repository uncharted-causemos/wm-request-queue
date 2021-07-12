package queue

import "sync"

// RequestQueue defines an interface for a basic FIFO queue.
type RequestQueue interface {
	Enqueue(x interface{}) bool
	EnqueueHashed(key int, x interface{}) bool
	Dequeue() interface{}
	Size() int
}

// SimpleRequestQueue is a baseline FIFO queue implementation that relies on a single
// buffered channel.
type SimpleRequestQueue struct {
	queue  chan queuedItem
	hashes map[int]bool
	size   int
	mutex  sync.RWMutex // channels are overkill here
}

type queuedItem struct {
	key int
	x   interface{}
}

// NewSimpleRequestQueue create a new SimpleRequestQueue that is immediately ready to
// receive enqueue requests.  The size of the queue is limited by the `size` parameter.
func NewSimpleRequestQueue(size int) RequestQueue {
	return &SimpleRequestQueue{
		queue:  make(chan queuedItem, size),
		hashes: map[int]bool{},
		size:   size,
		mutex:  sync.RWMutex{},
	}
}

// Enqueue adds a new item to the queue.  If the queue is full, the item will not
// be added, and the function will return `false`.
func (r *SimpleRequestQueue) Enqueue(x interface{}) bool {
	select {
	case r.queue <- queuedItem{x: x}:
		return true
	default:
		return false
	}
}

// EnqueueHashed adds a new item to the queue if an item with a similar hash doesn't already exist.
// If the queue is full, the item will not be added, and the function will return `false`.  If an
func (r *SimpleRequestQueue) EnqueueHashed(key int, x interface{}) bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if !r.hashes[key] {
		select {
		case r.queue <- queuedItem{key: key, x: x}:
			r.hashes[key] = true
			return true
		default:
			return false
		}
	}
	return true
}

// Dequeue removes an item from the queue.  If the queue is empty, the operation blocks.
func (r *SimpleRequestQueue) Dequeue() interface{} {
	result := <-r.queue
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.hashes[result.key] {
		delete(r.hashes, result.key)
	}
	return result.x
}

// Size returns the curent size of the queue.
func (r *SimpleRequestQueue) Size() int {
	return len(r.queue)
}
