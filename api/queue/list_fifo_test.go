package queue

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestListEnqueueDequeue(t *testing.T) {
	queue := NewListFIFOQueue(2)

	result := queue.Enqueue(10)
	assert.True(t, result)
	result = queue.Enqueue(20)
	assert.True(t, result)
	result = queue.Enqueue(30)
	assert.False(t, result)

	count := queue.Size()
	assert.Equal(t, 2, count)

	dequeueResult := queue.Dequeue().(int)
	assert.Equal(t, 10, dequeueResult)

	count = queue.Size()
	assert.Equal(t, 1, count)

	dequeueResult = queue.Dequeue().(int)
	assert.Equal(t, 20, dequeueResult)

	count = queue.Size()
	assert.Equal(t, 0, count)
}

func TestListBlockingDequeue(t *testing.T) {
	queue := NewListFIFOQueue(2)

	queue.Enqueue(10)
	queue.Enqueue(20)
	queue.Dequeue()
	queue.Dequeue()

	// setup a dequeue in a different go routine
	done := make(chan bool)
	var dequeueResult int
	go func() {
		dequeueResult = queue.Dequeue().(int)
		done <- true
	}()

	// force a bit of a wait to ensure that the dequeue is blocked, then
	// enqueue
	time.Sleep(1 * time.Second)
	queue.Enqueue(30)

	// wait until the dequeue is done
	<-done

	assert.Equal(t, 30, dequeueResult)
	assert.Equal(t, 0, queue.Size())
}

func TestHashedEnqueueDequeue(t *testing.T) {
	queue := NewListFIFOQueue(2)

	// ensure request with identical keys are only added once
	result := queue.EnqueueHashed(1, 10)
	assert.True(t, result)
	result = queue.EnqueueHashed(1, 10)
	assert.True(t, result)
	count := queue.Size()
	assert.Equal(t, 1, count)
	result = queue.EnqueueHashed(2, 20)
	assert.True(t, result)

	// ensure that dequeing requests will allow a follow on request
	// with the same key to be added
	dequeueResult := queue.Dequeue().(int)
	assert.Equal(t, 10, dequeueResult)
	dequeueResult = queue.Dequeue().(int)
	assert.Equal(t, 20, dequeueResult)

	count = queue.Size()
	assert.Equal(t, 0, count)

	result = queue.EnqueueHashed(1, 10)
	assert.True(t, result)

	count = queue.Size()
	assert.Equal(t, 1, count)
}