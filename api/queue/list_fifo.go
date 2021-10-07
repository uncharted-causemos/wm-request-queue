package queue

import (
	"container/list"
	"reflect"
	"sync"

	"github.com/pkg/errors"
)

// RequestQueue defines an interface for a request queue that supports enqueuing and dequeuing operations.
type RequestQueue interface {
	Enqueue(x interface{}) (bool, error)
	EnqueueHashed(key int, x interface{}) (bool, error)
	Dequeue() (interface{}, error)
	Clear() error
	Close() error
	Size() int
	GetAll() ([]interface{}, error)
}

type queuedItem struct {
	Key   int
	Value interface{}
}

// ListFIFOQueue is a FIFO queue implementation based on a doubly linked list.
type ListFIFOQueue struct {
	queue  *list.List
	hashes map[int]bool
	size   int
	closed bool
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
		closed: false,
		mutex:  mutex,
		cond:   sync.NewCond(mutex),
	}

	return srQueue
}

// Enqueue adds a new item to the queue.  If the queue is full, the item will not
// be added, and the function will return `false`.
func (r *ListFIFOQueue) Enqueue(x interface{}) (bool, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.closed {
		return false, errors.New("no enqueue after close")
	}

	if r.queue.Len() < r.size {
		// add data
		r.queue.PushBack(&queuedItem{Value: x})
		r.cond.Signal()
		return true, nil
	}
	return false, nil
}

// EnqueueHashed adds a new item to the queue if an item with a similar hash doesn't already exist.
// If the queue is full, the item will not be added, and the function will return `false`.  If an
func (r *ListFIFOQueue) EnqueueHashed(key int, x interface{}) (bool, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.closed {
		return false, errors.New("no enqueue after close")
	}

	if !r.hashes[key] {
		if r.queue.Len() < r.size {
			r.queue.PushBack(&queuedItem{Value: x, Key: key})
			r.hashes[key] = true
			// signal that there's data available
			r.cond.Signal()
			return true, nil
		}
		return false, nil
	}
	return true, nil
}

// Dequeue removes an item from the queue.  If the queue is empty, the operation blocks.
func (r *ListFIFOQueue) Dequeue() (interface{}, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.closed {
		return false, errors.New("no dequeue after close")
	}

	// wait until there's data
	for r.queue.Len() == 0 {
		r.cond.Wait()
	}

	// peek at the data
	result := r.queue.Front()
	value := result.Value.(*queuedItem)

	// remove the item from the queue and the hash set if necessary
	r.queue.Remove(result)
	if r.hashes[value.Key] {
		delete(r.hashes, value.Key)
	}

	return value.Value, nil
}

// Size returns the curent size of the queue.
func (r *ListFIFOQueue) Size() int {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	return r.queue.Len()
}

// Clear clears the queue and request key hash map.
func (r *ListFIFOQueue) Clear() error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.closed {
		return errors.New("no queue clear after close")
	}

	r.queue.Init()
	r.hashes = map[int]bool{}

	return nil
}

// Close closes the queue forbidding further operations.
func (r *ListFIFOQueue) Close() error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.closed {
		return errors.New("no close of previously closed queue")
	}

	r.closed = true
	return nil
}

// GetAll retrieves all the contents in the queue
func (r *ListFIFOQueue) GetAll() ([]interface{}, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	listCopy := make([]interface{}, r.queue.Len())
	current := r.queue.Front()
	i := 0
	for current != nil {
		item, ok := current.Value.(*queuedItem)
		if !ok {
			return nil, errors.Errorf("unexpected type %s", reflect.TypeOf(item))
		}
		listCopy[i] = item.Value
		i++
		current = current.Next()
	}
	return listCopy, nil
}
