package queue

import (
	"encoding/gob"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPersistedEnqueueDequeue(t *testing.T) {

	t.Cleanup(func() {
		err := os.RemoveAll(path.Join("test_data", "q1"))
		assert.NoError(t, err)
	})

	queue, err := NewPersistedFIFOQueue(2, "test_data", "q1")
	assert.NoError(t, err)

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

func TestPersistedBlockingDequeue(t *testing.T) {
	t.Cleanup(func() {
		err := os.RemoveAll(path.Join("test_data", "q2"))
		assert.NoError(t, err)
	})

	queue, err := NewPersistedFIFOQueue(2, "test_data", "q2")
	assert.NoError(t, err)

	_, _ = queue.Enqueue(10)
	_, _ = queue.Enqueue(20)
	_, _ = queue.Dequeue()
	_, _ = queue.Dequeue()

	// setup a dequeue in a different go routine
	done := make(chan bool)

	// force a bit of a wait to ensure that the dequeue is blocked, then
	// enqueue
	_, err = queue.Enqueue(30)

	var dequeueResult interface{}
	go func() {
		dequeueResult, err = queue.Dequeue()
		assert.NoError(t, err)
		done <- true
	}()

	// wait until the dequeue is done
	<-done
	assert.NoError(t, err)

	assert.Equal(t, 30, dequeueResult.(int))
	assert.Equal(t, 0, queue.Size())
}

func TestPersistedHashedEnqueueDequeue(t *testing.T) {
	t.Cleanup(func() {
		err := os.RemoveAll(path.Join("test_data", "q3"))
		assert.NoError(t, err)
	})

	queue, err := NewPersistedFIFOQueue(2, "test_data", "q3")
	assert.NoError(t, err)

	// ensure request with identical keys are only added once
	result, err := queue.EnqueueHashed(1, 10)
	assert.True(t, result)
	assert.NoError(t, err)
	result, err = queue.EnqueueHashed(1, 10)
	assert.True(t, result)
	assert.NoError(t, err)
	count := queue.Size()
	assert.Equal(t, 1, count)
	result, err = queue.EnqueueHashed(2, 20)
	assert.True(t, result)
	assert.NoError(t, err)

	// ensure that dequeing requests will allow a follow on request
	// with the same key to be added
	dequeueResult, err := queue.Dequeue()

	assert.Equal(t, 10, dequeueResult.(int))
	assert.NoError(t, err)
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

func TestPersistedListClear(t *testing.T) {
	t.Cleanup(func() {
		err := os.RemoveAll(path.Join("test_data", "q4"))
		assert.NoError(t, err)
	})

	queue, err := NewPersistedFIFOQueue(3, "test_data", "q4")
	assert.NoError(t, err)
	_, _ = queue.Enqueue(10)
	_, _ = queue.Enqueue(20)
	_, _ = queue.Enqueue(30)
	err = queue.Clear()
	assert.NoError(t, err)

	count := queue.Size()
	assert.Equal(t, 0, count)
}

func TestPersistedListLoad(t *testing.T) {
	t.Cleanup(func() {
		err := os.RemoveAll(path.Join("test_data", "q5"))
		assert.NoError(t, err)
	})

	queue, err := NewPersistedFIFOQueue(3, "test_data", "q5")
	assert.NoError(t, err)
	_, _ = queue.EnqueueHashed(10, 1000)
	_, _ = queue.EnqueueHashed(20, 2000)
	_, _ = queue.EnqueueHashed(30, 3000)
	queue.Close()

	queue, err = NewPersistedFIFOQueue(3, "test_data", "q5")
	assert.NoError(t, err)
	count := queue.Size()
	assert.Equal(t, 3, count)

	result, err := queue.EnqueueHashed(10, 1000)
	assert.NoError(t, err)
	assert.True(t, result)

	result, err = queue.EnqueueHashed(40, 4000)
	assert.NoError(t, err)
	assert.False(t, result)
}

func TestPersistedListClose(t *testing.T) {
	queue, _ := NewPersistedFIFOQueue(3, "test_data", "q6")
	_, _ = queue.Enqueue(10)
	_, _ = queue.Enqueue(20)
	_, _ = queue.Enqueue(30)

	err := queue.Close()
	assert.NoError(t, err)

	err = queue.Close()
	assert.Error(t, err)

	err = queue.Clear()
	assert.NoError(t, err)

	_, err = queue.Enqueue(10)
	assert.Error(t, err)

	_, err = queue.EnqueueHashed(10, 100)
	assert.Error(t, err)

	_, err = queue.Dequeue()
	assert.Error(t, err)
}

type testObject struct {
	Value int
}

func TestPersistedInerfaceEnqueue(t *testing.T) {

	gob.Register(testObject{})

	t.Cleanup(func() {
		err := os.RemoveAll(path.Join("test_data", "q1"))
		assert.NoError(t, err)
	})

	queue, err := NewPersistedFIFOQueue(2, "test_data", "q1")
	assert.NoError(t, err)

	result, err := queue.Enqueue(testObject{10})
	assert.NoError(t, err)
	assert.True(t, result)
	result, err = queue.Enqueue(testObject{20})
	assert.NoError(t, err)
	assert.True(t, result)

	count := queue.Size()
	assert.Equal(t, 2, count)
}
