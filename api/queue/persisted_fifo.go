package queue

import (
	"os"
	"path"
	"reflect"
	"sync"

	"github.com/pkg/errors"
	"github.com/uncharted-causemos/dque"
	"gitlab.uncharted.software/WM/wm-request-queue/config"
)

const queueSegmenSize = 50

// PersistedFIFOQueue is a FIFO queue implementation based on a doubly linked list.
type PersistedFIFOQueue struct {
	config.Config
	queue  *dque.DQue
	size   int
	hashes map[int]bool
	mutex  *sync.RWMutex
}

func queuedItemBuilder() interface{} {
	return &queuedItem{}
}

// KeyMapBuilder stores the queue idempotency keys that are deserialized from the persisted
// dque on startup.
type KeyMapBuilder struct {
	KeyMap map[int]bool
}

// Apply is called on each item of the persisted queue when it is loaded from disk, storing the
// returned item idempotency keys in an in-memory set.
func (k *KeyMapBuilder) Apply(entry interface{}) error {
	request, ok := entry.(*queuedItem)
	if !ok {
		return errors.Errorf("unexpected type %s", reflect.TypeOf(entry))
	}
	k.KeyMap[int(request.Key)] = true
	return nil
}

// NewPersistedFIFOQueue create a new ServiceRequestQueue that is immediately ready to
// receive enqueue requests.  The size of the queue is limited by the `size` parameter.
func NewPersistedFIFOQueue(size int, queueDir string, queueName string) (RequestQueue, error) {
	mutex := &sync.RWMutex{}

	queuePath := path.Join(queueDir, queueName)

	var queue *dque.DQue
	if _, err := os.Stat(queuePath); err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(queueDir, os.ModePerm)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to create request queue dir %s", queueDir)
			}

			queue, err = dque.New(queueName, queueDir, queueSegmenSize, queuedItemBuilder)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to initialize request queue %s/%s", queueDir, queueName)
			}
		}
	} else {
		queue, err = dque.Open(queueName, queueDir, queueSegmenSize, queuedItemBuilder)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to load request queue %s/%s", queueDir, queueName)
		}
	}

	mapBuilder := KeyMapBuilder{KeyMap: map[int]bool{}}
	err := queue.ApplyToQueue(&mapBuilder)
	if err != nil {
		return nil, errors.Wrapf(err, "failed rebuild key set for %s/%s", queueDir, queueName)
	}

	srQueue := &PersistedFIFOQueue{
		queue:  queue,
		size:   size,
		hashes: mapBuilder.KeyMap,
		mutex:  mutex,
	}

	return srQueue, nil
}

// Enqueue adds a new item to the queue.  If the queue is full, the item will not
// be added, and the function will return `false`.
func (r *PersistedFIFOQueue) Enqueue(x interface{}) (bool, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.queue.Size() < r.size {
		if err := r.queue.Enqueue(&queuedItem{Value: x}); err != nil {
			return false, errors.Wrap(err, "failed to enqueue")
		}
		return true, nil
	}
	return false, nil
}

// EnqueueHashed adds a new item to the queue if an item with a similar hash doesn't already exist.
// If the queue is full, the item will not be added, and the function will return `false`.  If an entry
// already exists, the item won't be added, but true will still be returned.
func (r *PersistedFIFOQueue) EnqueueHashed(key int, x interface{}) (bool, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if !r.hashes[key] {
		if r.queue.Size() < r.size {
			if err := r.queue.Enqueue(&queuedItem{Value: x, Key: key}); err != nil {
				return false, errors.Wrap(err, "failed to enqueue with hash key")
			}
			r.hashes[key] = true
			return true, nil
		}
		return false, nil
	}
	return true, nil
}

// Dequeue removes an item from the queue.  If the queue is empty, the operation blocks.
func (r *PersistedFIFOQueue) Dequeue() (interface{}, error) {
	result, err := r.queue.DequeueBlock()
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if err != nil {
		return nil, errors.Wrap(err, "failed to dequeue")
	}

	value := result.(*queuedItem)

	delete(r.hashes, value.Key)

	return value.Value, nil
}

// Size returns the curent size of the queue.
func (r *PersistedFIFOQueue) Size() int {
	return r.queue.Size()
}

// Clear clears the queue
func (r *PersistedFIFOQueue) Clear() error {
	// the underlying queue has no clear function so our only option is to drain it
	// iteratively
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// clear the key map
	r.hashes = map[int]bool{}

	count := r.queue.Size()
	for i := 0; i < count; i++ {
		_, err := r.queue.Dequeue()
		if err != nil {
			return errors.Wrap(err, "failed to clear queue")
		}
	}

	return nil
}

// Close closes the queue, flushes state to disk, and disallows any further operations.
func (r *PersistedFIFOQueue) Close() error {
	return errors.Wrap(r.queue.Close(), "failed to close queue")
}

// Contents is used to extract items in the persisted queue
type Contents struct {
	Jobs  []interface{}
	Index int
}

// Apply is called on each element of the queue each time the contents of the queue
// must be read
func (q *Contents) Apply(entry interface{}) error {
	request, ok := entry.(*queuedItem)
	if !ok {
		return errors.Errorf("unexpected type %s", reflect.TypeOf(entry))
	}
	q.Jobs[q.Index] = request.Value
	q.Index++

	return nil
}

// GetAll retrieves all of the contents in the queue
func (r *PersistedFIFOQueue) GetAll() ([]interface{}, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	queueContents := Contents{Jobs: make([]interface{}, r.queue.Size()), Index: 0}
	err := r.queue.ApplyToQueue(&queueContents)
	if err != nil {
		return nil, err
	}
	return queueContents.Jobs, nil
}
