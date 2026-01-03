package scheduler

import (
	"context"
	"errors"
	"sync"

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
	ListPendingJobsCalls   []struct{ MaxTriggerTime, Limit int64 }
	UpdateStatusCalls      []struct{ Id uint64; Status entity.JobStatus }
	UpdateNextTriggerCalls []struct{ Id uint64; NextTrigger int64 }
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
	m.UpdateStatusCalls = append(m.UpdateStatusCalls, struct{ Id uint64; Status entity.JobStatus }{id, status})
	m.mu.Unlock()

	if m.UpdateStatusFunc != nil {
		return m.UpdateStatusFunc(ctx, id, status)
	}
	return nil
}

func (m *MockJobRepo) UpdateNextTriggerTime(ctx context.Context, id uint64, nextTrigger int64) error {
	m.mu.Lock()
	m.UpdateNextTriggerCalls = append(m.UpdateNextTriggerCalls, struct{ Id uint64; NextTrigger int64 }{id, nextTrigger})
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
	ExecuteFunc func(ctx context.Context, config *entity.JobPayload) (*executor.Result, error)
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
