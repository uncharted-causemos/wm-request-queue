package queue

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEnqueueDequeue(t *testing.T) {
	queue := NewServiceRequestQueue(2)

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

func TestBlockingDequeue(t *testing.T) {
	queue := NewServiceRequestQueue(2)

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
