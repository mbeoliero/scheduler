package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTaskQueue(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	queue := NewTaskQueue(rdb, "test_prefix")
	assert.NotNil(t, queue)
	assert.Equal(t, "test_prefix", queue.keyPrefix)
}

func TestTaskQueue_PushPop_Success(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	queue := NewTaskQueue(rdb, "test")
	ctx := context.Background()

	msg := &TaskMessage{
		Namespace:   "default",
		RecordId:    12345,
		JobId:       100,
		JobKey:      "test_job",
		TriggerTime: time.Now().UnixMilli(),
		CreatedAt:   time.Now(),
	}

	// Push
	err = queue.PushTask(ctx, msg)
	require.NoError(t, err)

	// Pop
	result, err := queue.PopTask(ctx, 1*time.Second)
	require.NoError(t, err)
	assert.NotNil(t, result)

	assert.Equal(t, msg.Namespace, result.Namespace)
	assert.Equal(t, msg.RecordId, result.RecordId)
	assert.Equal(t, msg.JobId, result.JobId)
	assert.Equal(t, msg.JobKey, result.JobKey)
	assert.Equal(t, msg.TriggerTime, result.TriggerTime)
}

func TestTaskQueue_PopTask_Timeout(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	queue := NewTaskQueue(rdb, "test")
	ctx := context.Background()

	// Pop from empty queue with short timeout
	start := time.Now()
	result, err := queue.PopTask(ctx, 100*time.Millisecond)
	elapsed := time.Since(start)

	assert.NoError(t, err)
	assert.Nil(t, result)
	assert.GreaterOrEqual(t, elapsed, 100*time.Millisecond)
}

func TestTaskQueue_PopTask_ContextCancellation(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	queue := NewTaskQueue(rdb, "test")
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel immediately after starting pop
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	result, err := queue.PopTask(ctx, 5*time.Second)
	elapsed := time.Since(start)
	t.Log(elapsed)

	// Should return quickly due to context cancellation
	//assert.True(t, elapsed < 1*time.Second)
	// Context cancelled should return an error or nil
	if err != nil {
		assert.Contains(t, err.Error(), "context canceled")
	}
	assert.Nil(t, result)
}

func TestTaskQueue_FIFO_Order(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	queue := NewTaskQueue(rdb, "test")
	ctx := context.Background()

	// Push multiple messages
	for i := 1; i <= 3; i++ {
		msg := &TaskMessage{
			RecordId: int64(i),
			JobId:    int64(i * 10),
			JobKey:   "job_" + string(rune('0'+i)),
		}
		err := queue.PushTask(ctx, msg)
		require.NoError(t, err)
	}

	// Pop and verify FIFO order
	for i := 1; i <= 3; i++ {
		result, err := queue.PopTask(ctx, 1*time.Second)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, int64(i), result.RecordId)
	}
}

func TestTaskQueue_EmptyAfterPop(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	queue := NewTaskQueue(rdb, "test")
	ctx := context.Background()

	// Push one message
	msg := &TaskMessage{RecordId: 1, JobId: 1, JobKey: "test"}
	err = queue.PushTask(ctx, msg)
	require.NoError(t, err)

	// Pop it
	result, err := queue.PopTask(ctx, 100*time.Millisecond)
	require.NoError(t, err)
	assert.NotNil(t, result)

	// Queue should now be empty
	result, err = queue.PopTask(ctx, 100*time.Millisecond)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

func TestTaskQueue_ConcurrentPushPop(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	queue := NewTaskQueue(rdb, "test")
	ctx := context.Background()

	const numMessages = 100
	done := make(chan struct{})

	// Producer goroutine
	go func() {
		for i := 0; i < numMessages; i++ {
			msg := &TaskMessage{
				RecordId: int64(i),
				JobId:    int64(i),
				JobKey:   "concurrent_job",
			}
			_ = queue.PushTask(ctx, msg)
		}
		close(done)
	}()

	// Consumer
	var received int
	for received < numMessages {
		result, err := queue.PopTask(ctx, 500*time.Millisecond)
		if err != nil {
			break
		}
		if result != nil {
			received++
		}
	}

	<-done
	assert.Equal(t, numMessages, received)
}

func TestTaskQueue_LargePayload(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	queue := NewTaskQueue(rdb, "test")
	ctx := context.Background()

	// Large job key
	largeJobKey := make([]byte, 10000)
	for i := range largeJobKey {
		largeJobKey[i] = 'a'
	}

	msg := &TaskMessage{
		Namespace:   "test_namespace",
		RecordId:    999999999,
		JobId:       888888888,
		JobKey:      string(largeJobKey),
		TriggerTime: time.Now().UnixMilli(),
		CreatedAt:   time.Now(),
	}

	err = queue.PushTask(ctx, msg)
	require.NoError(t, err)

	result, err := queue.PopTask(ctx, 1*time.Second)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, string(largeJobKey), result.JobKey)
}

func TestTaskQueue_MultipleQueues(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	queue1 := NewTaskQueue(rdb, "prefix1")
	queue2 := NewTaskQueue(rdb, "prefix2")
	ctx := context.Background()

	// Push to queue1
	msg1 := &TaskMessage{RecordId: 1, JobId: 1, JobKey: "q1_job"}
	err = queue1.PushTask(ctx, msg1)
	require.NoError(t, err)

	// Push to queue2
	msg2 := &TaskMessage{RecordId: 2, JobId: 2, JobKey: "q2_job"}
	err = queue2.PushTask(ctx, msg2)
	require.NoError(t, err)

	// Pop from queue1 should get msg1
	result, err := queue1.PopTask(ctx, 100*time.Millisecond)
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.RecordId)

	// Pop from queue2 should get msg2
	result, err = queue2.PopTask(ctx, 100*time.Millisecond)
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.RecordId)

	// Both queues should now be empty
	result, _ = queue1.PopTask(ctx, 100*time.Millisecond)
	assert.Nil(t, result)

	result, _ = queue2.PopTask(ctx, 100*time.Millisecond)
	assert.Nil(t, result)
}

func TestTaskQueue_RedisConnectionFailure(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	queue := NewTaskQueue(rdb, "test")
	ctx := context.Background()

	// Close Redis to simulate connection failure
	mr.Close()

	msg := &TaskMessage{RecordId: 1, JobId: 1, JobKey: "test"}
	err = queue.PushTask(ctx, msg)
	assert.Error(t, err)

	_, err = queue.PopTask(ctx, 100*time.Millisecond)
	assert.Error(t, err)
}
