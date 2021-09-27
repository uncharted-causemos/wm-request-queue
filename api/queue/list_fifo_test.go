package queue

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestListEnqueueDequeue(t *testing.T) {
	queue := NewListFIFOQueue(2)

	result, err := queue.Enqueue(10)
	assert.NoError(t, err)
	assert.True(t, result)
	result, err = queue.Enqueue(20)
	assert.NoError(t, err)
	assert.True(t, result)
	result, err = queue.Enqueue(30)
	assert.NoError(t, err)
	assert.False(t, result)

	count := queue.Size()
	assert.Equal(t, 2, count)

	dequeueResult, err := queue.Dequeue()
	assert.NoError(t, err)
	assert.Equal(t, 10, dequeueResult.(int))

	count = queue.Size()
	assert.Equal(t, 1, count)

	dequeueResult, err = queue.Dequeue()
	assert.NoError(t, err)
	assert.Equal(t, 20, dequeueResult.(int))

	count = queue.Size()
	assert.Equal(t, 0, count)
}

func TestListBlockingDequeue(t *testing.T) {
	queue := NewListFIFOQueue(2)

	_, _ = queue.Enqueue(10)
	_, _ = queue.Enqueue(20)
	_, _ = queue.Dequeue()
	_, _ = queue.Dequeue()

	// setup a dequeue in a different go routine
	done := make(chan bool)
	var dequeueResult interface{}
	go func() {
		dequeueResult, _ = queue.Dequeue()
		done <- true
	}()

	// force a bit of a wait to ensure that the dequeue is blocked, then
	// enqueue
	time.Sleep(1 * time.Second)
	_, _ = queue.Enqueue(30)

	// wait until the dequeue is done
	<-done

	assert.Equal(t, 30, dequeueResult.(int))
	assert.Equal(t, 0, queue.Size())
}

func TestHashedEnqueueDequeue(t *testing.T) {
	queue := NewListFIFOQueue(2)

	// ensure request with identical keys are only added once
	result, err := queue.EnqueueHashed(1, 10)
	assert.NoError(t, err)
	assert.True(t, result)
	result, err = queue.EnqueueHashed(1, 10)
	assert.NoError(t, err)
	assert.True(t, result)
	count := queue.Size()
	assert.Equal(t, 1, count)
	result, err = queue.EnqueueHashed(2, 20)
	assert.NoError(t, err)
	assert.True(t, result)

	// ensure that dequeing requests will allow a follow on request
	// with the same key to be added
	dequeueResult, err := queue.Dequeue()
	assert.NoError(t, err)
	assert.Equal(t, 10, dequeueResult.(int))
	dequeueResult, err = queue.Dequeue()
	assert.NoError(t, err)
	assert.Equal(t, 20, dequeueResult.(int))

	count = queue.Size()
	assert.Equal(t, 0, count)

	result, err = queue.EnqueueHashed(1, 10)
	assert.NoError(t, err)
	assert.True(t, result)

	count = queue.Size()
	assert.Equal(t, 1, count)
}

func TestListClear(t *testing.T) {
	queue := NewListFIFOQueue(2)
	_, _ = queue.Enqueue(10)
	_, _ = queue.Enqueue(20)
	_, _ = queue.Enqueue(30)

	err := queue.Clear()
	assert.NoError(t, err)

	count := queue.Size()
	assert.Equal(t, 0, count)
}

func TestListClose(t *testing.T) {
	queue := NewListFIFOQueue(2)
	_, _ = queue.Enqueue(10)
	_, _ = queue.Enqueue(20)
	_, _ = queue.Enqueue(30)

	err := queue.Close()
	assert.NoError(t, err)

	err = queue.Close()
	assert.Error(t, err)

	err = queue.Clear()
	assert.Error(t, err)

	_, err = queue.Enqueue(10)
	assert.Error(t, err)

	_, err = queue.EnqueueHashed(10, 100)
	assert.Error(t, err)

	_, err = queue.Dequeue()
	assert.Error(t, err)
}
