package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewElector_Validation(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr:        mr.Addr(),
		MaxRetries:  -1,
		DialTimeout: 50 * time.Millisecond,
	})
	defer func() { _ = rdb.Close() }()

	t.Run("empty nodeId returns error", func(t *testing.T) {
		_, err = NewElector(rdb, "", "election_key")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "nodeId is empty")
	})

	t.Run("empty electionKey returns error", func(t *testing.T) {
		_, err = NewElector(rdb, "node1", "")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "electionKey is empty")
	})
}

func TestNewElector_DefaultOptions(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	t.Run("default expiry and autoExtendDur when not set", func(t *testing.T) {
		e, err := NewElector(rdb, "node1", "election_key")
		require.NoError(t, err)

		// Default autoExtendDur is 3s, expiry is autoExtendDur * 2 = 6s
		assert.Equal(t, 3*time.Second, e.autoExtendDur)
		assert.Equal(t, 6*time.Second, e.expiry)
	})

	t.Run("expiry set, autoExtendDur derived", func(t *testing.T) {
		e, err := NewElector(rdb, "node1", "election_key", WithExpiry(10*time.Second))
		require.NoError(t, err)

		// autoExtendDur = expiry / 2
		assert.Equal(t, 5*time.Second, e.autoExtendDur)
		assert.Equal(t, 10*time.Second, e.expiry)
	})

	t.Run("autoExtendDur set, expiry derived", func(t *testing.T) {
		e, err := NewElector(rdb, "node1", "election_key", WithAutoExtendDuration(4*time.Second))
		require.NoError(t, err)

		// expiry = autoExtendDur * 2
		assert.Equal(t, 4*time.Second, e.autoExtendDur)
		assert.Equal(t, 8*time.Second, e.expiry)
	})

	t.Run("both set explicitly", func(t *testing.T) {
		e, err := NewElector(rdb, "node1", "election_key",
			WithExpiry(15*time.Second),
			WithAutoExtendDuration(7*time.Second))
		require.NoError(t, err)

		assert.Equal(t, 7*time.Second, e.autoExtendDur)
		assert.Equal(t, 15*time.Second, e.expiry)
	})

	t.Run("key prefix applied", func(t *testing.T) {
		e, err := NewElector(rdb, "node1", "election_key", WithLockPrefix("myprefix"))
		require.NoError(t, err)

		assert.Equal(t, "myprefix", e.keyPrefix)
		assert.Equal(t, "myprefix:election_key", e.getKey("election_key"))
	})
}

func TestElector_Start_AlreadyStarted(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	e, err := NewElector(rdb, "node1", "election_key",
		WithExpiry(100*time.Millisecond),
		WithAutoExtendDuration(50*time.Millisecond))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// First start should succeed
	err = e.Start(ctx)
	assert.NoError(t, err)
	defer e.Stop()

	// Second start should return ErrAlreadyStarted
	err = e.Start(ctx)
	assert.Equal(t, ErrAlreadyStarted, err)
}

func TestElector_BecomeLeader(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	e, err := NewElector(rdb, "node1", "election_key",
		WithExpiry(500*time.Millisecond),
		WithAutoExtendDuration(200*time.Millisecond))
	require.NoError(t, err)

	ctx := context.Background()

	err = e.Start(ctx)
	require.NoError(t, err)
	defer e.Stop()

	// Wait for election
	time.Sleep(100 * time.Millisecond)

	assert.True(t, e.isLeader.Load())
	assert.Equal(t, "node1", e.GetLeaderId())
	assert.NoError(t, e.IsLeader(ctx))
}

func TestElector_IsLeader_NotLeader(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	e, err := NewElector(rdb, "node1", "election_key")
	require.NoError(t, err)

	// Not started, should not be leader
	err = e.IsLeader(context.Background())
	assert.Equal(t, ErrNotLeader, err)
}

func TestElector_Resign(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	e, err := NewElector(rdb, "node1", "election_key",
		WithExpiry(500*time.Millisecond),
		WithAutoExtendDuration(200*time.Millisecond))
	require.NoError(t, err)

	ctx := context.Background()

	err = e.Start(ctx)
	require.NoError(t, err)

	// Wait for election
	time.Sleep(100 * time.Millisecond)
	assert.True(t, e.isLeader.Load())

	// Stop should resign
	e.Stop()

	assert.False(t, e.isLeader.Load())
	assert.Equal(t, "", e.GetLeaderId())
}

func TestElector_Stop_NotStarted(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	e, err := NewElector(rdb, "node1", "election_key")
	require.NoError(t, err)

	// Stop without start should not panic
	e.Stop()
}

func TestElector_ConcurrentCampaign(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	const numNodes = 5
	var electors []*Elector
	ctx := context.Background()

	// Create multiple electors
	for i := 0; i < numNodes; i++ {
		e, err := NewElector(rdb, "node"+string(rune('0'+i)), "shared_election_key",
			WithExpiry(500*time.Millisecond),
			WithAutoExtendDuration(200*time.Millisecond))
		require.NoError(t, err)
		electors = append(electors, e)
	}

	// Start all concurrently
	var wg sync.WaitGroup
	for _, e := range electors {
		wg.Add(1)
		go func(elector *Elector) {
			defer wg.Done()
			_ = elector.Start(ctx)
		}(e)
	}
	wg.Wait()

	// Allow time for election
	time.Sleep(300 * time.Millisecond)

	// Count leaders
	leaderCount := 0
	for _, e := range electors {
		if e.isLeader.Load() {
			leaderCount++
		}
	}

	// Exactly one leader
	assert.Equal(t, 1, leaderCount, "exactly one node should be leader")

	// Cleanup
	for _, e := range electors {
		e.Stop()
	}
}

func TestElector_LoseLeadership_ExtendFail(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	// Don't close mr yet, we'll close it to simulate failure

	rdb := redis.NewClient(&redis.Options{
		Addr:        mr.Addr(),
		MaxRetries:  3,
		DialTimeout: 50 * time.Millisecond,
	})
	defer func() { _ = rdb.Close() }()

	e, err := NewElector(rdb, "node1", "election_key",
		WithExpiry(200*time.Millisecond),
		WithAutoExtendDuration(50*time.Millisecond))
	require.NoError(t, err)

	ctx := context.Background()

	err = e.Start(ctx)
	require.NoError(t, err)
	defer e.Stop()

	// Wait for election
	time.Sleep(100 * time.Millisecond)
	assert.True(t, e.isLeader.Load())

	// Close Redis to simulate network failure
	mr.Close()

	// Wait for lock to expire and refresh to fail
	time.Sleep(300 * time.Millisecond)

	// Should have lost leadership
	assert.False(t, e.isLeader.Load())
}

func TestElector_ContextCancellation(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	e, err := NewElector(rdb, "node1", "election_key",
		WithExpiry(500*time.Millisecond),
		WithAutoExtendDuration(200*time.Millisecond))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	err = e.Start(ctx)
	require.NoError(t, err)

	// Wait for election
	time.Sleep(100 * time.Millisecond)
	assert.True(t, e.isLeader.Load())

	// Cancel context should trigger resign
	cancel()

	// Wait for campaign goroutine to exit
	time.Sleep(100 * time.Millisecond)

	assert.False(t, e.isLeader.Load())
}

func TestElector_GetKey(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	t.Run("without prefix", func(t *testing.T) {
		e, err := NewElector(rdb, "node1", "election_key")
		require.NoError(t, err)

		assert.Equal(t, "test_key", e.getKey("test_key"))
	})

	t.Run("with prefix", func(t *testing.T) {
		e, err := NewElector(rdb, "node1", "election_key", WithLockPrefix("scheduler"))
		require.NoError(t, err)

		assert.Equal(t, "scheduler:test_key", e.getKey("test_key"))
	})
}

func TestElector_LeadershipTransfer(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	// First elector becomes leader
	e1, err := NewElector(rdb, "node1", "election_key",
		WithExpiry(200*time.Millisecond),
		WithAutoExtendDuration(50*time.Millisecond))
	require.NoError(t, err)

	ctx := context.Background()

	err = e1.Start(ctx)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)
	assert.True(t, e1.isLeader.Load())

	// Second elector waiting
	e2, err := NewElector(rdb, "node2", "election_key",
		WithExpiry(200*time.Millisecond),
		WithAutoExtendDuration(50*time.Millisecond))
	require.NoError(t, err)

	err = e2.Start(ctx)
	require.NoError(t, err)
	defer e2.Stop()

	time.Sleep(1000 * time.Millisecond)
	assert.False(t, e2.isLeader.Load()) // e2 shouldn't be leader while e1 holds the lock
	assert.True(t, e1.isLeader.Load())

	// Stop e1, e2 should become leader
	e1.Stop()

	// Wait for e2 to acquire leadership
	var becameLeader atomic.Bool
	for i := 0; i < 10; i++ {
		time.Sleep(100 * time.Millisecond)
		if e2.isLeader.Load() {
			becameLeader.Store(true)
			break
		}
	}

	assert.True(t, becameLeader.Load(), "node2 should have become leader after node1 stopped")
}
