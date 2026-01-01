package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"

	"github.com/mbeoliero/scheduler/pkg/log"
)

var (
	ErrNotLeader      = errors.New("not the leader")
	ErrAlreadyStarted = errors.New("elector already started")
)

// Elector implements leader election using Redis
type Elector struct {
	rs            *redsync.Redsync
	options       []redsync.Option
	mutex         *redsync.Mutex
	autoExtendDur time.Duration
	expiry        time.Duration
	keyPrefix     string
	electionKey   string
	mu            sync.RWMutex
	isLeader      atomic.Bool
	nodeId        string
	leaderId      string
	stopCh        chan struct{}
	doneCh        chan struct{}
	started       bool
}

// Option is a function that configures an Elector
type Option func(*Elector)

// WithRedsyncOptions sets the redsync options.
func WithRedsyncOptions(options ...redsync.Option) Option {
	return func(e *Elector) {
		e.options = options
	}
}

// WithLockPrefix sets the prefix for lock keys
func WithLockPrefix(prefix string) Option {
	return func(e *Elector) {
		e.keyPrefix = prefix
	}
}

func WithExpiry(expiry time.Duration) Option {
	return func(e *Elector) {
		e.expiry = expiry
	}
}

func WithAutoExtendDuration(duration time.Duration) Option {
	return func(e *Elector) {
		e.autoExtendDur = duration
	}
}

// NewElector creates a new Redis-based elector
func NewElector(r redis.UniversalClient, nodeId string, electionKey string, opts ...Option) (*Elector, error) {
	if len(nodeId) == 0 {
		return nil, fmt.Errorf("nodeId is empty")
	}
	if len(electionKey) == 0 {
		return nil, fmt.Errorf("electionKey is empty")
	}

	pool := goredis.NewPool(r)
	rs := redsync.New(pool)

	e := &Elector{
		rs:          rs,
		nodeId:      nodeId,
		electionKey: electionKey,
		stopCh:      make(chan struct{}),
		doneCh:      make(chan struct{}),
	}
	for _, opt := range opts {
		opt(e)
	}

	if e.autoExtendDur == 0 {
		if e.expiry != 0 {
			e.autoExtendDur = e.expiry / 2
		} else {
			e.autoExtendDur = 3 * time.Second
		}
	}
	if e.expiry == 0 {
		e.expiry = e.autoExtendDur * 2
	}

	e.options = append(e.options, redsync.WithExpiry(e.expiry))
	return e, nil
}

func (e *Elector) getKey(key string) string {
	if e.keyPrefix == "" {
		return key
	}
	return fmt.Sprintf("%s:%s", e.keyPrefix, key)
}

// Start begins the leader election process
func (e *Elector) Start(ctx context.Context) error {
	e.mu.Lock()
	if e.started {
		e.mu.Unlock()
		return ErrAlreadyStarted
	}

	e.started = true
	e.mu.Unlock()

	go e.campaign(ctx, e.getKey(e.electionKey))

	return nil
}

func (e *Elector) campaign(ctx context.Context, lockName string) {
	defer close(e.doneCh)
	defer func() {
		if r := recover(); r != nil {
			log.Error("elector campaign recovered from panic, node id: %s, err: %v", e.nodeId, r)
		}
	}()

	// 立即尝试一次
	e.tryBecomeLeader(lockName)

	ticker := time.NewTicker(e.autoExtendDur)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			e.resign()
			return
		case <-ctx.Done():
			e.resign()
			return
		case <-ticker.C:
			if !e.isLeader.Load() {
				e.tryBecomeLeader(lockName)
			} else {
				if !e.tryExtendLock() {
					e.tryBecomeLeader(lockName)
				}
			}
		}
	}
}

func (e *Elector) tryBecomeLeader(lockName string) {
	if e.isLeader.Load() {
		return
	}

	e.mu.Lock()
	if e.mutex == nil {
		e.mutex = e.rs.NewMutex(lockName, e.options...)
	}
	mutex := e.mutex
	e.mu.Unlock()

	err := mutex.Lock()
	e.mu.Lock()
	defer e.mu.Unlock()

	if err != nil {
		wasLeader := e.isLeader.Swap(false)
		e.leaderId = ""
		if wasLeader {
			log.Info("lost leadership, node id: %s, err: %v", e.nodeId, err)
		}
		return
	}

	wasLeader := e.isLeader.Swap(true)
	e.leaderId = e.nodeId
	if !wasLeader {
		log.Info("became leader, node id: %s", e.nodeId)
	}
}

func (e *Elector) tryExtendLock() bool {
	e.mu.RLock()
	mutex := e.mutex
	e.mu.RUnlock()

	if mutex == nil {
		return false
	}

	ok, err := mutex.Extend()
	if err != nil || !ok {
		log.Error("failed to extend lock, node id: %s, err: %v", e.nodeId, err)

		e.mu.Lock()
		e.isLeader.Store(false)
		e.leaderId = ""
		e.mutex = nil
		e.mu.Unlock()

		log.Info("lost leadership, node id: %s", e.nodeId)
		return false
	}
	return true
}

// resign releases the lock if we're the leader
func (e *Elector) resign() {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.mutex != nil {
		_, err := e.mutex.Unlock()
		if err != nil {
			log.Error("failed to unlock, node id: %s, err: %v", e.nodeId, err)
		}
		e.mutex = nil
	}

	e.isLeader.Store(false)
	e.leaderId = ""
	log.Info("resigned from leadership, node id: %s", e.nodeId)
}

// IsLeader checks if this instance is the current leader
func (e *Elector) IsLeader(ctx context.Context) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.isLeader.Load() {
		return ErrNotLeader
	}
	return nil
}

// GetLeaderId returns the current leader's identifier
func (e *Elector) GetLeaderId() string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.leaderId
}

// Stop stops the election process
func (e *Elector) Stop() {
	e.mu.Lock()
	if !e.started {
		e.mu.Unlock()
		return
	}
	e.started = false
	close(e.stopCh)
	e.mu.Unlock()

	select {
	case <-e.doneCh:
	case <-time.After(5 * time.Second):
		log.Warn("election process did not complete in time, node id: %s", e.nodeId)
	}
}
