package scheduler

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mbeoliero/scheduler/domain/entity"
	"github.com/mbeoliero/scheduler/internal/executor"
)

// MockJobRepo implements repo.JobRepo for testing
type MockJobRepo struct {
	mu                    sync.Mutex
	jobs                  map[uint64]*entity.Job
	ListPendingJobsFunc   func(ctx context.Context, maxTriggerTime int64, limit int64) ([]*entity.Job, error)
	FindByIdFunc          func(ctx context.Context, id uint64) (*entity.Job, error)
	UpdateStatusFunc      func(ctx context.Context, id uint64, status entity.JobStatus) error
	UpdateNextTriggerFunc func(ctx context.Context, id uint64, nextTrigger int64) error
	CreateFunc            func(ctx context.Context, job *entity.Job) error
	UpdateFunc            func(ctx context.Context, job *entity.Job) error
	DeleteFunc            func(ctx context.Context, id uint64) error

	// Call tracking
	ListPendingJobsCalls []struct{ MaxTriggerTime, Limit int64 }
	UpdateStatusCalls    []struct {
		Id     uint64
		Status entity.JobStatus
	}
	UpdateNextTriggerCalls []struct {
		Id          uint64
		NextTrigger int64
	}
}

func NewMockJobRepo() *MockJobRepo {
	return &MockJobRepo{
		jobs: make(map[uint64]*entity.Job),
	}
}

func (m *MockJobRepo) Create(ctx context.Context, job *entity.Job) error {
	if m.CreateFunc != nil {
		return m.CreateFunc(ctx, job)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs[job.Id] = job
	return nil
}

func (m *MockJobRepo) Update(ctx context.Context, job *entity.Job) error {
	if m.UpdateFunc != nil {
		return m.UpdateFunc(ctx, job)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs[job.Id] = job
	return nil
}

func (m *MockJobRepo) Delete(ctx context.Context, id uint64) error {
	if m.DeleteFunc != nil {
		return m.DeleteFunc(ctx, id)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.jobs, id)
	return nil
}

func (m *MockJobRepo) FindById(ctx context.Context, id uint64) (*entity.Job, error) {
	if m.FindByIdFunc != nil {
		return m.FindByIdFunc(ctx, id)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	job, ok := m.jobs[id]
	if !ok {
		return nil, nil
	}
	return job, nil
}

func (m *MockJobRepo) ListPendingJobs(ctx context.Context, maxTriggerTime int64, limit int64) ([]*entity.Job, error) {
	m.mu.Lock()
	m.ListPendingJobsCalls = append(m.ListPendingJobsCalls, struct{ MaxTriggerTime, Limit int64 }{maxTriggerTime, limit})
	m.mu.Unlock()

	if m.ListPendingJobsFunc != nil {
		return m.ListPendingJobsFunc(ctx, maxTriggerTime, limit)
	}
	return nil, nil
}

func (m *MockJobRepo) UpdateStatus(ctx context.Context, id uint64, status entity.JobStatus) error {
	m.mu.Lock()
	m.UpdateStatusCalls = append(m.UpdateStatusCalls, struct {
		Id     uint64
		Status entity.JobStatus
	}{id, status})
	m.mu.Unlock()

	if m.UpdateStatusFunc != nil {
		return m.UpdateStatusFunc(ctx, id, status)
	}
	return nil
}

func (m *MockJobRepo) UpdateNextTriggerTime(ctx context.Context, id uint64, nextTrigger int64) error {
	m.mu.Lock()
	m.UpdateNextTriggerCalls = append(m.UpdateNextTriggerCalls, struct {
		Id          uint64
		NextTrigger int64
	}{id, nextTrigger})
	m.mu.Unlock()

	if m.UpdateNextTriggerFunc != nil {
		return m.UpdateNextTriggerFunc(ctx, id, nextTrigger)
	}
	return nil
}

// MockJobRecordRepo implements repo.JobRecordRepo for testing
type MockJobRecordRepo struct {
	mu               sync.Mutex
	records          map[uint64]*entity.JobRecord
	CreateFunc       func(ctx context.Context, record *entity.JobRecord) error
	UpdateFunc       func(ctx context.Context, record *entity.JobRecord) error
	FindByIdFunc     func(ctx context.Context, id uint64) (*entity.JobRecord, error)
	UpdateStatusFunc func(ctx context.Context, id uint64, status entity.JobRecordStatus, result string) error
	DeleteFunc       func(ctx context.Context, id uint64) error

	// Call tracking
	CreateCalls       []*entity.JobRecord
	UpdateStatusCalls []struct {
		Id     uint64
		Status entity.JobRecordStatus
		Result string
	}
}

func NewMockJobRecordRepo() *MockJobRecordRepo {
	return &MockJobRecordRepo{
		records: make(map[uint64]*entity.JobRecord),
	}
}

func (m *MockJobRecordRepo) Create(ctx context.Context, record *entity.JobRecord) error {
	m.mu.Lock()
	m.CreateCalls = append(m.CreateCalls, record)
	m.mu.Unlock()

	if m.CreateFunc != nil {
		return m.CreateFunc(ctx, record)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records[record.Id] = record
	return nil
}

func (m *MockJobRecordRepo) Update(ctx context.Context, record *entity.JobRecord) error {
	if m.UpdateFunc != nil {
		return m.UpdateFunc(ctx, record)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records[record.Id] = record
	return nil
}

func (m *MockJobRecordRepo) FindById(ctx context.Context, id uint64) (*entity.JobRecord, error) {
	if m.FindByIdFunc != nil {
		return m.FindByIdFunc(ctx, id)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.records[id], nil
}

func (m *MockJobRecordRepo) UpdateStatus(ctx context.Context, id uint64, status entity.JobRecordStatus, result string) error {
	m.mu.Lock()
	m.UpdateStatusCalls = append(m.UpdateStatusCalls, struct {
		Id     uint64
		Status entity.JobRecordStatus
		Result string
	}{id, status, result})
	m.mu.Unlock()

	if m.UpdateStatusFunc != nil {
		return m.UpdateStatusFunc(ctx, id, status, result)
	}
	return nil
}

func (m *MockJobRecordRepo) Delete(ctx context.Context, id uint64) error {
	if m.DeleteFunc != nil {
		return m.DeleteFunc(ctx, id)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.records, id)
	return nil
}

// MockExecutor implements executor.Executor for testing
type MockExecutor struct {
	ExecuteFunc  func(ctx context.Context, config *entity.JobPayload) (*executor.Result, error)
	ExecuteCalls []*entity.JobPayload
	mu           sync.Mutex
}

func NewMockExecutor() *MockExecutor {
	return &MockExecutor{}
}

func (m *MockExecutor) Execute(ctx context.Context, config *entity.JobPayload) (*executor.Result, error) {
	m.mu.Lock()
	m.ExecuteCalls = append(m.ExecuteCalls, config)
	m.mu.Unlock()

	if m.ExecuteFunc != nil {
		return m.ExecuteFunc(ctx, config)
	}
	return &executor.Result{Success: true, Output: "mock output"}, nil
}

// ErrMock is a generic error for testing
var ErrMock = errors.New("mock error")

// AtomicExecutionTracker tracks job executions in a thread-safe manner for distributed testing
type AtomicExecutionTracker struct {
	mu            sync.Mutex
	executions    map[string][]time.Time // jobKey -> execution timestamps
	executeCounts map[string]*atomic.Int64
}

func NewAtomicExecutionTracker() *AtomicExecutionTracker {
	return &AtomicExecutionTracker{
		executions:    make(map[string][]time.Time),
		executeCounts: make(map[string]*atomic.Int64),
	}
}

func (t *AtomicExecutionTracker) RecordExecution(jobKey string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.executions[jobKey] = append(t.executions[jobKey], time.Now())

	if _, ok := t.executeCounts[jobKey]; !ok {
		t.executeCounts[jobKey] = &atomic.Int64{}
	}
	t.executeCounts[jobKey].Add(1)
}

func (t *AtomicExecutionTracker) GetExecutionCount(jobKey string) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	if counter, ok := t.executeCounts[jobKey]; ok {
		return counter.Load()
	}
	return 0
}

func (t *AtomicExecutionTracker) GetAllExecutionCounts() map[string]int64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	result := make(map[string]int64)
	for key, counter := range t.executeCounts {
		result[key] = counter.Load()
	}
	return result
}

func (t *AtomicExecutionTracker) GetTotalExecutions() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	var total int64
	for _, counter := range t.executeCounts {
		total += counter.Load()
	}
	return total
}

// TestExecutor is a configurable executor for distributed testing
type TestExecutor struct {
	mu           sync.Mutex
	tracker      *AtomicExecutionTracker
	delay        time.Duration
	shouldFail   bool
	failureRate  float64 // 0.0 to 1.0
	executeCalls []*entity.JobPayload
}

func NewTestExecutor(tracker *AtomicExecutionTracker) *TestExecutor {
	return &TestExecutor{
		tracker: tracker,
	}
}

func (e *TestExecutor) WithDelay(d time.Duration) *TestExecutor {
	e.delay = d
	return e
}

func (e *TestExecutor) WithFailure(shouldFail bool) *TestExecutor {
	e.shouldFail = shouldFail
	return e
}

func (e *TestExecutor) Execute(ctx context.Context, config *entity.JobPayload) (*executor.Result, error) {
	e.mu.Lock()
	e.executeCalls = append(e.executeCalls, config)
	e.mu.Unlock()

	// Simulate execution delay
	if e.delay > 0 {
		select {
		case <-time.After(e.delay):
		case <-ctx.Done():
			return &executor.Result{Success: false, Error: ctx.Err().Error()}, ctx.Err()
		}
	}

	// Track execution by job key (extracted from payload if available)
	if config != nil && e.tracker != nil {
		jobKey := "unknown"
		if config.Http != nil && config.Http.Url != "" {
			jobKey = config.Http.Url
		}
		e.tracker.RecordExecution(jobKey)
	}

	if e.shouldFail {
		return &executor.Result{Success: false, Error: "simulated failure"}, nil
	}

	return &executor.Result{Success: true, Output: "test execution completed"}, nil
}

func (e *TestExecutor) GetExecuteCalls() []*entity.JobPayload {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.executeCalls
}

// SharedMockJobRepo is a thread-safe job repo that can be shared across multiple schedulers
type SharedMockJobRepo struct {
	mu                   sync.RWMutex
	jobs                 map[uint64]*entity.Job
	ListPendingJobsFunc  func(ctx context.Context, maxTriggerTime int64, limit int64) ([]*entity.Job, error)
	listPendingCallCount atomic.Int64

	// Status tracking
	statusUpdates []struct {
		Id     uint64
		Status entity.JobStatus
	}
	triggerTimeUpdates []struct {
		Id          uint64
		NextTrigger int64
	}
}

func NewSharedMockJobRepo() *SharedMockJobRepo {
	return &SharedMockJobRepo{
		jobs: make(map[uint64]*entity.Job),
	}
}

func (m *SharedMockJobRepo) Create(ctx context.Context, job *entity.Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs[job.Id] = job
	return nil
}

func (m *SharedMockJobRepo) Update(ctx context.Context, job *entity.Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs[job.Id] = job
	return nil
}

func (m *SharedMockJobRepo) Delete(ctx context.Context, id uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.jobs, id)
	return nil
}

func (m *SharedMockJobRepo) FindById(ctx context.Context, id uint64) (*entity.Job, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	job, ok := m.jobs[id]
	if !ok {
		return nil, nil
	}
	// Return a copy to avoid data races
	jobCopy := *job
	return &jobCopy, nil
}

func (m *SharedMockJobRepo) ListPendingJobs(ctx context.Context, maxTriggerTime int64, limit int64) ([]*entity.Job, error) {
	m.listPendingCallCount.Add(1)

	if m.ListPendingJobsFunc != nil {
		return m.ListPendingJobsFunc(ctx, maxTriggerTime, limit)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*entity.Job
	for _, job := range m.jobs {
		if job.NextTriggerTime <= maxTriggerTime && job.Status == entity.JobStatusActive {
			jobCopy := *job
			result = append(result, &jobCopy)
		}
	}
	return result, nil
}

func (m *SharedMockJobRepo) UpdateStatus(ctx context.Context, id uint64, status entity.JobStatus) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statusUpdates = append(m.statusUpdates, struct {
		Id     uint64
		Status entity.JobStatus
	}{id, status})
	if job, ok := m.jobs[id]; ok {
		job.Status = status
	}
	return nil
}

func (m *SharedMockJobRepo) UpdateNextTriggerTime(ctx context.Context, id uint64, nextTrigger int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.triggerTimeUpdates = append(m.triggerTimeUpdates, struct {
		Id          uint64
		NextTrigger int64
	}{id, nextTrigger})
	if job, ok := m.jobs[id]; ok {
		job.NextTriggerTime = nextTrigger
	}
	return nil
}

func (m *SharedMockJobRepo) GetListPendingCallCount() int64 {
	return m.listPendingCallCount.Load()
}

func (m *SharedMockJobRepo) AddJob(job *entity.Job) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs[job.Id] = job
}

// SharedMockJobRecordRepo is a thread-safe record repo that can be shared across multiple schedulers
type SharedMockJobRecordRepo struct {
	mu            sync.RWMutex
	records       map[uint64]*entity.JobRecord
	createCount   atomic.Int64
	statusUpdates []struct {
		Id     uint64
		Status entity.JobRecordStatus
		Result string
	}
}

func NewSharedMockJobRecordRepo() *SharedMockJobRecordRepo {
	return &SharedMockJobRecordRepo{
		records: make(map[uint64]*entity.JobRecord),
	}
}

func (m *SharedMockJobRecordRepo) Create(ctx context.Context, record *entity.JobRecord) error {
	m.createCount.Add(1)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records[record.Id] = record
	return nil
}

func (m *SharedMockJobRecordRepo) Update(ctx context.Context, record *entity.JobRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records[record.Id] = record
	return nil
}

func (m *SharedMockJobRecordRepo) FindById(ctx context.Context, id uint64) (*entity.JobRecord, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.records[id], nil
}

func (m *SharedMockJobRecordRepo) UpdateStatus(ctx context.Context, id uint64, status entity.JobRecordStatus, result string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statusUpdates = append(m.statusUpdates, struct {
		Id     uint64
		Status entity.JobRecordStatus
		Result string
	}{id, status, result})
	if record, ok := m.records[id]; ok {
		record.JobStatus = status
	}
	return nil
}

func (m *SharedMockJobRecordRepo) Delete(ctx context.Context, id uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.records, id)
	return nil
}

func (m *SharedMockJobRecordRepo) GetCreateCount() int64 {
	return m.createCount.Load()
}

func (m *SharedMockJobRecordRepo) GetSuccessCount() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var count int64
	for _, update := range m.statusUpdates {
		if update.Status == entity.JobRecordStatusSuccess {
			count++
		}
	}
	return count
}
