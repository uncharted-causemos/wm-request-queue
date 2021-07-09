package queue

// RequestQueue defines an interface for a basic FIFO queue.
type RequestQueue interface {
	Enqueue(x interface{}) bool
	Dequeue() interface{}
	Size() int
}

// SimpleRequestQueue is a baseline FIFO queue implementation that relies on a single
// buffered channel.
type SimpleRequestQueue struct {
	enqueueChan chan interface{}
}

// NewSimpleRequestQueue create a new SimpleRequestQueue that is immediately ready to
// receive enqueue requests.  The size of the queue is limited by the `size` parameter.
func NewSimpleRequestQueue(size int) RequestQueue {
	return &SimpleRequestQueue{
		enqueueChan: make(chan interface{}, size),
	}
}

// Enqueue adds a new item to the queue.  If the queue is full, the item will not
// be added, and the function will return `false`.
func (r *SimpleRequestQueue) Enqueue(x interface{}) bool {
	// Non-blocking - request is added to the queue if there's room otherwise
	// returns false
	select {
	case r.enqueueChan <- x:
		return true
	default:
		return false
	}
}

// Dequeue removes an item from the queue.  If the queue is empty, the operation blocks.
func (r *SimpleRequestQueue) Dequeue() interface{} {
	request := <-r.enqueueChan
	return request
}

// Size returns the curent size of the queue.
func (r *SimpleRequestQueue) Size() int {
	return len(r.enqueueChan)
}
