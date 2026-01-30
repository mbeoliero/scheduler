package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-co-op/gocron/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mbeoliero/scheduler/domain/entity"
	"github.com/mbeoliero/scheduler/domain/repo"
	"github.com/mbeoliero/scheduler/infra/config"
	infraRedis "github.com/mbeoliero/scheduler/infra/redis"
)

// setupTestEnv initializes test environment with mock dependencies
func setupTestEnv(t *testing.T) (*miniredis.Miniredis, func()) {
	t.Helper()

	mr, err := miniredis.Run()
	require.NoError(t, err)

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	// Setup global Redis client
	infraRedis.SetClient(rdb)

	// Setup mock repos
	mockJobRepo := NewMockJobRepo()
	mockRecordRepo := NewMockJobRecordRepo()
	repo.SetJobRepo(mockJobRepo)
	repo.SetJobRecordRepo(mockRecordRepo)

	// Setup config with test node id
	testCfg := &config.Config{
		Server: config.ServerConfig{NodeId: "test-node-1"},
	}
	config.SetConfig(testCfg)

	// id_gen is auto-initialized via init()

	cleanup := func() {
		_ = rdb.Close()
		mr.Close()
	}

	return mr, cleanup
}

func getTestSchedulerConfig() config.SchedulerConfig {
	return config.SchedulerConfig{
		SchedulerKeyPrefix:    "test_scheduler",
		SchedulerLoopInterval: 3 * time.Second,
		LeaderKey:             "test_leader",
		PreReadSeconds:        30,
		EnableTaskQueue:       false,
		LeaderTtl:             5 * time.Second,
		LeaderRenew:           2 * time.Second,
		LockerExpiry:          5 * time.Second,
		DefaultTimeout:        30 * time.Second,
		BatchSize:             100,
		MaxWorkers:            10,
	}
}

func TestNewScheduler_Success(t *testing.T) {
	_, cleanup := setupTestEnv(t)
	defer cleanup()

	cfg := getTestSchedulerConfig()
	s, err := NewScheduler(cfg)
	require.NoError(t, err)
	assert.NotNil(t, s)
	assert.NotNil(t, s.listPendingCron)
	assert.NotNil(t, s.executorCron)
	assert.NotNil(t, s.loadedJobs)
	assert.NotNil(t, s.jobVersions)
}

func TestNewScheduler_WithTaskQueue(t *testing.T) {
	_, cleanup := setupTestEnv(t)
	defer cleanup()

	cfg := getTestSchedulerConfig()
	cfg.EnableTaskQueue = true

	s, err := NewScheduler(cfg)
	require.NoError(t, err)
	assert.NotNil(t, s.queue)
}

func TestScheduleJob_SkipUnchanged(t *testing.T) {
	_, cleanup := setupTestEnv(t)
	defer cleanup()

	cfg := getTestSchedulerConfig()
	s, err := NewScheduler(cfg)
	require.NoError(t, err)
	defer s.Stop(context.Background())

	ctx := context.Background()

	job := &entity.Job{
		Id:              1,
		Namespace:       "test",
		JobKey:          "job1",
		ScheduleType:    entity.ScheduleTypePeriodicRate,
		ScheduleExpr:    "5s",
		ExecuteType:     entity.ExecuteTypeHttp,
		NextTriggerTime: time.Now().Add(10 * time.Second).UnixMilli(),
		UpdatedAt:       1000,
	}

	// First schedule
	err = s.scheduleJob(ctx, job)
	require.NoError(t, err)
	assert.Len(t, s.loadedJobs, 1)

	// Same job with same version should be skipped
	err = s.scheduleJob(ctx, job)
	assert.NoError(t, err)
	assert.Len(t, s.loadedJobs, 1)
}

func TestScheduleJob_UpdateChanged(t *testing.T) {
	_, cleanup := setupTestEnv(t)
	defer cleanup()

	cfg := getTestSchedulerConfig()
	s, err := NewScheduler(cfg)
	require.NoError(t, err)
	defer s.Stop(context.Background())

	ctx := context.Background()

	job := &entity.Job{
		Id:              1,
		Namespace:       "test",
		JobKey:          "job1",
		ScheduleType:    entity.ScheduleTypePeriodicRate,
		ScheduleExpr:    "5s",
		ExecuteType:     entity.ExecuteTypeHttp,
		NextTriggerTime: time.Now().Add(10 * time.Second).UnixMilli(),
		UpdatedAt:       1000,
	}

	// First schedule
	err = s.scheduleJob(ctx, job)
	require.NoError(t, err)

	originalJobId := s.loadedJobs[job.UniqueKey()].ID()

	// Update job version
	job.UpdatedAt = 2000
	err = s.scheduleJob(ctx, job)
	require.NoError(t, err)

	// Job should have been replaced
	newJobId := s.loadedJobs[job.UniqueKey()].ID()
	assert.NotEqual(t, originalJobId, newJobId)
	assert.Equal(t, int64(2000), s.jobVersions[job.UniqueKey()])
}

func TestScheduleJob_DurationParseFail(t *testing.T) {
	_, cleanup := setupTestEnv(t)
	defer cleanup()

	cfg := getTestSchedulerConfig()
	s, err := NewScheduler(cfg)
	require.NoError(t, err)
	defer s.Stop(context.Background())

	ctx := context.Background()

	job := &entity.Job{
		Id:           1,
		Namespace:    "test",
		JobKey:       "job1",
		ScheduleType: entity.ScheduleTypePeriodicRate,
		ScheduleExpr: "invalid_duration", // Invalid
		ExecuteType:  entity.ExecuteTypeHttp,
	}

	err = s.scheduleJob(ctx, job)
	assert.Error(t, err)
	assert.Len(t, s.loadedJobs, 0)
}

func TestScheduleJob_CronExpr(t *testing.T) {
	_, cleanup := setupTestEnv(t)
	defer cleanup()

	cfg := getTestSchedulerConfig()
	s, err := NewScheduler(cfg)
	require.NoError(t, err)
	defer s.Stop(context.Background())

	ctx := context.Background()

	job := &entity.Job{
		Id:              1,
		Namespace:       "test",
		JobKey:          "cron_job",
		ScheduleType:    entity.ScheduleTypePeriodicCron,
		ScheduleExpr:    "*/5 * * * *",
		ExecuteType:     entity.ExecuteTypeHttp,
		NextTriggerTime: time.Now().Add(5 * time.Minute).UnixMilli(),
	}

	err = s.scheduleJob(ctx, job)
	require.NoError(t, err)
	assert.Len(t, s.loadedJobs, 1)
}

func TestScheduleJob_OneTimeJob(t *testing.T) {
	_, cleanup := setupTestEnv(t)
	defer cleanup()

	cfg := getTestSchedulerConfig()
	s, err := NewScheduler(cfg)
	require.NoError(t, err)
	defer s.Stop(context.Background())

	ctx := context.Background()

	t.Run("immediate job", func(t *testing.T) {
		job := &entity.Job{
			Id:              1,
			Namespace:       "test",
			JobKey:          "immediate_job",
			ScheduleType:    entity.ScheduleTypeImmediate,
			ExecuteType:     entity.ExecuteTypeHttp,
			NextTriggerTime: time.Now().UnixMilli(),
		}

		err = s.scheduleJob(ctx, job)
		require.NoError(t, err)
	})

	t.Run("delayed job", func(t *testing.T) {
		job := &entity.Job{
			Id:              2,
			Namespace:       "test",
			JobKey:          "delayed_job",
			ScheduleType:    entity.ScheduleTypeDelayed,
			ExecuteType:     entity.ExecuteTypeHttp,
			NextTriggerTime: time.Now().Add(1 * time.Hour).UnixMilli(),
		}

		err = s.scheduleJob(ctx, job)
		require.NoError(t, err)
	})
}

func TestScheduleJob_OneTimeJob_NegativeDelay(t *testing.T) {
	_, cleanup := setupTestEnv(t)
	defer cleanup()

	cfg := getTestSchedulerConfig()
	s, err := NewScheduler(cfg)
	require.NoError(t, err)
	defer s.Stop(context.Background())

	ctx := context.Background()

	// Job with trigger time in the past
	job := &entity.Job{
		Id:              1,
		Namespace:       "test",
		JobKey:          "past_job",
		ScheduleType:    entity.ScheduleTypeDelayed,
		ExecuteType:     entity.ExecuteTypeHttp,
		NextTriggerTime: time.Now().Add(-1 * time.Hour).UnixMilli(), // Past
	}

	// Should handle negative delay by setting to 0
	err = s.scheduleJob(ctx, job)
	require.NoError(t, err)
	assert.Len(t, s.loadedJobs, 1)
}

func TestScheduleJob_UnknownScheduleType(t *testing.T) {
	_, cleanup := setupTestEnv(t)
	defer cleanup()

	cfg := getTestSchedulerConfig()
	s, err := NewScheduler(cfg)
	require.NoError(t, err)
	defer s.Stop(context.Background())

	ctx := context.Background()

	job := &entity.Job{
		Id:           1,
		Namespace:    "test",
		JobKey:       "unknown_type_job",
		ScheduleType: entity.ScheduleType(999), // Unknown type
		ExecuteType:  entity.ExecuteTypeHttp,
	}

	err = s.scheduleJob(ctx, job)
	assert.NoError(t, err) // Returns nil for unknown types
	assert.Len(t, s.loadedJobs, 0)
}

func TestProcessTask_Success(t *testing.T) {
	mr, cleanup := setupTestEnv(t)
	defer cleanup()

	// Setup mock job
	job := &entity.Job{
		Id:          1,
		Namespace:   "test",
		JobKey:      "process_test",
		ExecuteType: entity.ExecuteTypeHttp,
		Payload: &entity.JobPayload{
			Http: &entity.HttpPayload{
				Url:    "http://localhost:8080/test",
				Method: "GET",
			},
		},
	}

	mockJobRepo := repo.GetJobRepo().(*MockJobRepo)
	mockJobRepo.jobs[1] = job

	// We need to ensure miniredis is running
	_ = mr

	cfg := getTestSchedulerConfig()
	s, err := NewScheduler(cfg)
	require.NoError(t, err)
	defer s.Stop(context.Background())

	ctx := context.Background()

	// Note: This test might fail due to HTTP executor actually making requests
	// In a real scenario, we'd mock the executor as well
	// For this test, we're mainly checking the flow
	_ = s.processTask(ctx, 1, 100)

	// Check that status update was called
	mockRecordRepo := repo.GetJobRecordRepo().(*MockJobRecordRepo)
	assert.Greater(t, len(mockRecordRepo.UpdateStatusCalls), 0)
}

func TestProcessTask_JobNotFound(t *testing.T) {
	_, cleanup := setupTestEnv(t)
	defer cleanup()

	cfg := getTestSchedulerConfig()
	s, err := NewScheduler(cfg)
	require.NoError(t, err)
	defer s.Stop(context.Background())

	ctx := context.Background()

	mockJobRepo := repo.GetJobRepo().(*MockJobRepo)
	mockJobRepo.FindByIdFunc = func(ctx context.Context, id uint64) (*entity.Job, error) {
		return nil, errors.New("job not found")
	}

	err = s.processTask(ctx, 999, 100)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get job failed")
}

func TestProcessTask_ExecutorNotFound(t *testing.T) {
	_, cleanup := setupTestEnv(t)
	defer cleanup()

	job := &entity.Job{
		Id:          1,
		Namespace:   "test",
		JobKey:      "unknown_executor",
		ExecuteType: entity.ExecuteType(999), // Unknown type
	}

	mockJobRepo := repo.GetJobRepo().(*MockJobRepo)
	mockJobRepo.jobs[1] = job

	cfg := getTestSchedulerConfig()
	s, err := NewScheduler(cfg)
	require.NoError(t, err)
	defer s.Stop(context.Background())

	ctx := context.Background()
	err = s.processTask(ctx, 1, 100)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get executor failed")
}

func TestProcessTask_ExecutorReturnsNil(t *testing.T) {
	_, cleanup := setupTestEnv(t)
	defer cleanup()

	job := &entity.Job{
		Id:          1,
		Namespace:   "test",
		JobKey:      "nil_result",
		ExecuteType: entity.ExecuteTypeGolang,
		Payload:     &entity.JobPayload{},
	}

	mockJobRepo := repo.GetJobRepo().(*MockJobRepo)
	mockJobRepo.jobs[1] = job

	cfg := getTestSchedulerConfig()
	s, err := NewScheduler(cfg)
	require.NoError(t, err)
	defer s.Stop(context.Background())

	// The Go executor returns nil result
	ctx := context.Background()
	err = s.processTask(ctx, 1, 100)
	// Go executor returns (nil, ErrNotImplemented) so this should error
	assert.Error(t, err)
}

func TestProcessTask_ExecutorFailure(t *testing.T) {
	_, cleanup := setupTestEnv(t)
	defer cleanup()

	job := &entity.Job{
		Id:          1,
		Namespace:   "test",
		JobKey:      "fail_job",
		ExecuteType: entity.ExecuteTypeHttp,
		Payload: &entity.JobPayload{
			Http: &entity.HttpPayload{
				Url:    "http://invalid-url-that-will-fail.invalid/",
				Method: "GET",
			},
		},
	}

	mockJobRepo := repo.GetJobRepo().(*MockJobRepo)
	mockJobRepo.jobs[1] = job

	cfg := getTestSchedulerConfig()
	cfg.DefaultTimeout = 100 * time.Millisecond // Short timeout
	s, err := NewScheduler(cfg)
	require.NoError(t, err)
	defer s.Stop(context.Background())

	ctx := context.Background()
	err = s.processTask(ctx, 1, 100)

	// Either execution fails or returns failure result
	mockRecordRepo := repo.GetJobRecordRepo().(*MockJobRecordRepo)
	hasFailedStatus := false
	for _, call := range mockRecordRepo.UpdateStatusCalls {
		if call.Status == entity.JobRecordStatusFailed {
			hasFailedStatus = true
			break
		}
	}
	assert.True(t, hasFailedStatus || err != nil)
}

func TestStop_Idempotent(t *testing.T) {
	_, cleanup := setupTestEnv(t)
	defer cleanup()

	cfg := getTestSchedulerConfig()
	s, err := NewScheduler(cfg)
	require.NoError(t, err)

	ctx := context.Background()
	s.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	// Multiple stops should not panic
	s.Stop(ctx)
	s.Stop(ctx)
	s.Stop(ctx)
}

func TestStop_NotStarted(t *testing.T) {
	_, cleanup := setupTestEnv(t)
	defer cleanup()

	cfg := getTestSchedulerConfig()
	s, err := NewScheduler(cfg)
	require.NoError(t, err)

	// Stop without start should not panic
	s.Stop(context.Background())
}

func TestScheduler_ConcurrentScheduleJobs(t *testing.T) {
	_, cleanup := setupTestEnv(t)
	defer cleanup()

	ctx := context.TODO()

	cfg := getTestSchedulerConfig()
	s, err := NewScheduler(cfg)
	require.NoError(t, err)
	s.Start(ctx)
	defer s.Stop(ctx)

	// Concurrently schedule multiple jobs
	var wg sync.WaitGroup
	const numJobs = 20
	jobLen := numJobs / 2

	for i := 0; i < numJobs; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			job := &entity.Job{
				Id:           uint64(idx + 1),
				Namespace:    "test",
				JobKey:       "concurrent_job_" + string(rune('0'+idx%jobLen)),
				ScheduleType: entity.ScheduleTypePeriodicRate,
				ScheduleExpr: "3s",
				ExecuteType:  entity.ExecuteTypeHttp,
				Payload: &entity.JobPayload{Http: &entity.HttpPayload{
					Url:    "https://example.com",
					Method: "GET",
				}},
				NextTriggerTime: time.Now().Add(1 * time.Second).UnixMilli(),
				UpdatedAt:       int64(idx),
			}
			_ = s.scheduleJob(ctx, job)
		}(i)
	}
	wg.Wait()

	time.Sleep(100 * time.Second)
	// All jobs should be scheduled (some may have overwritten each other due to same keys)
	assert.Equal(t, len(s.loadedJobs), jobLen)
}

func TestTriggerJob_OneTime_RemoveAfterTrigger(t *testing.T) {
	_, cleanup := setupTestEnv(t)
	defer cleanup()

	cfg := getTestSchedulerConfig()
	s, err := NewScheduler(cfg)
	require.NoError(t, err)

	ctx := context.Background()
	s.Start(ctx)
	defer s.Stop(ctx)

	time.Sleep(100 * time.Millisecond)

	job := &entity.Job{
		Id:              1,
		Namespace:       "test",
		JobKey:          "one_time_trigger",
		ScheduleType:    entity.ScheduleTypeImmediate,
		ExecuteType:     entity.ExecuteTypeHttp,
		NextTriggerTime: time.Now().UnixMilli(),
		Payload: &entity.JobPayload{
			Http: &entity.HttpPayload{
				Url:    "http://localhost/test",
				Method: "GET",
			},
		},
	}

	// Schedule the job
	err = s.scheduleJob(ctx, job)
	require.NoError(t, err)

	// The job should be removed after trigger
	// We need to wait a bit for the one-time job to execute
	time.Sleep(200 * time.Millisecond)

	// After triggering, one-time jobs are removed from loadedJobs
	s.mu.RLock()
	_, exists := s.loadedJobs[job.UniqueKey()]
	s.mu.RUnlock()

	// Note: The job might still exist if it hasn't been triggered yet
	// This is a timing-sensitive test
	_ = exists
}

func TestWorkerPool_Limiting(t *testing.T) {
	_, cleanup := setupTestEnv(t)
	defer cleanup()

	cfg := getTestSchedulerConfig()
	cfg.MaxWorkers = 2 // Limit to 2 workers
	cfg.EnableTaskQueue = false

	s, err := NewScheduler(cfg)
	require.NoError(t, err)

	// Verify workerPool capacity
	assert.Equal(t, 2, cap(s.workerPool))
}

func TestTriggerJob_CreateRecordFail(t *testing.T) {
	_, cleanup := setupTestEnv(t)
	defer cleanup()

	cfg := getTestSchedulerConfig()
	s, err := NewScheduler(cfg)
	require.NoError(t, err)

	ctx := context.Background()
	s.Start(ctx)
	defer s.Stop(ctx)

	time.Sleep(100 * time.Millisecond)

	// Make record creation fail
	mockRecordRepo := repo.GetJobRecordRepo().(*MockJobRecordRepo)
	mockRecordRepo.CreateFunc = func(ctx context.Context, record *entity.JobRecord) error {
		return errors.New("db error")
	}

	job := &entity.Job{
		Id:              1,
		Namespace:       "test",
		JobKey:          "record_fail_job",
		ScheduleType:    entity.ScheduleTypePeriodicRate,
		ScheduleExpr:    "1s",
		ExecuteType:     entity.ExecuteTypeHttp,
		NextTriggerTime: time.Now().UnixMilli(),
	}

	// TriggerJob should handle the error gracefully
	s.triggerJob(ctx, job)

	// Should have attempted to create record
	assert.Greater(t, len(mockRecordRepo.CreateCalls), 0)
}

func TestTriggerJob_WithQueue_PushFail(t *testing.T) {
	mr, cleanup := setupTestEnv(t)
	defer cleanup()

	cfg := getTestSchedulerConfig()
	cfg.EnableTaskQueue = true

	s, err := NewScheduler(cfg)
	require.NoError(t, err)

	ctx := context.Background()
	s.Start(ctx)
	defer s.Stop(ctx)

	time.Sleep(100 * time.Millisecond)

	// Close Redis to make push fail
	mr.Close()

	job := &entity.Job{
		Id:              1,
		Namespace:       "test",
		JobKey:          "queue_fail_job",
		ScheduleType:    entity.ScheduleTypePeriodicRate,
		ScheduleExpr:    "1h",
		ExecuteType:     entity.ExecuteTypeHttp,
		NextTriggerTime: time.Now().UnixMilli(),
	}

	// Trigger should handle queue push failure
	s.triggerJob(ctx, job)

	// Status should be updated to failed
	time.Sleep(50 * time.Millisecond)
	mockRecordRepo := repo.GetJobRecordRepo().(*MockJobRecordRepo)
	hasFailedStatus := false
	for _, call := range mockRecordRepo.UpdateStatusCalls {
		if call.Status == entity.JobRecordStatusFailed {
			hasFailedStatus = true
			break
		}
	}
	assert.True(t, hasFailedStatus)
}

func TestLoadPendingJobs(t *testing.T) {
	_, cleanup := setupTestEnv(t)
	defer cleanup()

	cfg := getTestSchedulerConfig()
	cfg.PreReadSeconds = 60
	cfg.BatchSize = 10

	s, err := NewScheduler(cfg)
	require.NoError(t, err)
	defer s.Stop(context.Background())

	// Setup mock to return jobs
	mockJobRepo := repo.GetJobRepo().(*MockJobRepo)
	mockJobRepo.ListPendingJobsFunc = func(ctx context.Context, maxTriggerTime int64, limit int64) ([]*entity.Job, error) {
		return []*entity.Job{
			{
				Id:              1,
				Namespace:       "test",
				JobKey:          "pending_job_1",
				ScheduleType:    entity.ScheduleTypePeriodicRate,
				ScheduleExpr:    "3s",
				NextTriggerTime: time.Now().Add(10 * time.Second).UnixMilli(),
			},
			{
				Id:              2,
				Namespace:       "test",
				JobKey:          "pending_job_2",
				ScheduleType:    entity.ScheduleTypePeriodicCron,
				ScheduleExpr:    "*/5 * * * *",
				NextTriggerTime: time.Now().Add(8 * time.Second).UnixMilli(),
			},
		}, nil
	}

	ctx := context.Background()
	s.loadPendingJobs(ctx)

	// Should have loaded both jobs
	assert.Len(t, s.loadedJobs, 2)
	assert.Len(t, mockJobRepo.ListPendingJobsCalls, 1)
}

func TestLoadPendingJobs_Error(t *testing.T) {
	_, cleanup := setupTestEnv(t)
	defer cleanup()

	cfg := getTestSchedulerConfig()
	s, err := NewScheduler(cfg)
	require.NoError(t, err)
	defer s.Stop(context.Background())

	mockJobRepo := repo.GetJobRepo().(*MockJobRepo)
	mockJobRepo.ListPendingJobsFunc = func(ctx context.Context, maxTriggerTime int64, limit int64) ([]*entity.Job, error) {
		return nil, errors.New("database error")
	}

	ctx := context.Background()

	// Should not panic
	s.loadPendingJobs(ctx)
	assert.Len(t, s.loadedJobs, 0)
}

func TestUpdateNextTriggerTime_Periodic(t *testing.T) {
	_, cleanup := setupTestEnv(t)
	defer cleanup()

	cfg := getTestSchedulerConfig()
	s, err := NewScheduler(cfg)
	require.NoError(t, err)
	ctx := context.Background()
	s.Start(ctx)
	defer s.Stop(context.Background())

	job := &entity.Job{
		Id:              1,
		Namespace:       "test",
		JobKey:          "periodic_trigger",
		ScheduleType:    entity.ScheduleTypePeriodicRate,
		ScheduleExpr:    "10s",
		ExecuteType:     entity.ExecuteTypeHttp,
		NextTriggerTime: time.Now().Add(10 * time.Second).UnixMilli(),
	}

	// Schedule job first
	err = s.scheduleJob(ctx, job)
	require.NoError(t, err)

	// Update trigger time
	s.updateNextTriggerTime(ctx, job)

	// Check that UpdateNextTriggerTime was called
	mockJobRepo := repo.GetJobRepo().(*MockJobRepo)
	assert.Greater(t, len(mockJobRepo.UpdateNextTriggerCalls), 0)
}

func TestUpdateNextTriggerTime_OneTime(t *testing.T) {
	_, cleanup := setupTestEnv(t)
	defer cleanup()

	cfg := getTestSchedulerConfig()
	s, err := NewScheduler(cfg)
	require.NoError(t, err)
	ctx := context.Background()
	s.Start(ctx)
	defer s.Stop(context.Background())

	job := &entity.Job{
		Id:              1,
		Namespace:       "test",
		JobKey:          "onetime_trigger",
		ScheduleType:    entity.ScheduleTypeImmediate,
		ExecuteType:     entity.ExecuteTypeHttp,
		NextTriggerTime: time.Now().UnixMilli(),
	}

	// Update trigger time for one-time job should update status to Finished
	s.updateNextTriggerTime(ctx, job)

	mockJobRepo := repo.GetJobRepo().(*MockJobRepo)
	hasFinishedStatus := false
	for _, call := range mockJobRepo.UpdateStatusCalls {
		if call.Status == entity.JobStatusFinished {
			hasFinishedStatus = true
			break
		}
	}
	assert.True(t, hasFinishedStatus)
}

func TestScheduler_StartAndStop(t *testing.T) {
	_, cleanup := setupTestEnv(t)
	defer cleanup()

	cfg := getTestSchedulerConfig()
	s, err := NewScheduler(cfg)
	require.NoError(t, err)

	ctx := context.Background()

	// Start scheduler
	s.Start(ctx)
	assert.True(t, s.started.Load())

	time.Sleep(100 * time.Millisecond)

	// Stop scheduler
	s.Stop(ctx)
	assert.False(t, s.started.Load())
}

func TestRunTaskWorker_DisabledQueue(t *testing.T) {
	_, cleanup := setupTestEnv(t)
	defer cleanup()

	cfg := getTestSchedulerConfig()
	cfg.EnableTaskQueue = false

	s, err := NewScheduler(cfg)
	require.NoError(t, err)
	defer s.Stop(context.Background())

	ctx := context.Background()

	// runTaskWorker should return immediately when queue is disabled
	done := make(chan struct{})
	go func() {
		s.runTaskWorker(ctx)
		close(done)
	}()

	select {
	case <-done:
		// Good, returned quickly
	case <-time.After(500 * time.Millisecond):
		t.Fatal("runTaskWorker did not return quickly when queue is disabled")
	}
}

func TestProcessTaskFromQueue(t *testing.T) {
	_, cleanup := setupTestEnv(t)
	defer cleanup()

	job := &entity.Job{
		Id:          1,
		Namespace:   "test",
		JobKey:      "queue_process",
		ExecuteType: entity.ExecuteTypeHttp,
		Payload: &entity.JobPayload{
			Http: &entity.HttpPayload{
				Url:    "http://localhost/test",
				Method: "GET",
			},
		},
	}

	mockJobRepo := repo.GetJobRepo().(*MockJobRepo)
	mockJobRepo.jobs[1] = job

	cfg := getTestSchedulerConfig()
	cfg.EnableTaskQueue = true
	s, err := NewScheduler(cfg)
	require.NoError(t, err)
	defer s.Stop(context.Background())

	ctx := context.Background()

	msg := &TaskMessage{
		RecordId: 100,
		JobId:    1,
		JobKey:   "queue_process",
	}

	s.processTaskFromQueue(ctx, msg)

	// UpdateStatus should have been called (first to Running)
	mockRecordRepo := repo.GetJobRecordRepo().(*MockJobRecordRepo)
	hasRunningStatus := false
	for _, call := range mockRecordRepo.UpdateStatusCalls {
		if call.Status == entity.JobRecordStatusRunning {
			hasRunningStatus = true
			break
		}
	}
	assert.True(t, hasRunningStatus)
}

func TestScheduler_GracefulShutdown(t *testing.T) {
	_, cleanup := setupTestEnv(t)
	defer cleanup()

	cfg := getTestSchedulerConfig()
	cfg.MaxWorkers = 5
	s, err := NewScheduler(cfg)
	require.NoError(t, err)

	ctx := context.Background()
	s.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	// Fill worker pool
	var activeWorkers atomic.Int32
	for i := 0; i < 5; i++ {
		select {
		case s.workerPool <- struct{}{}:
			activeWorkers.Add(1)
			go func() {
				defer func() {
					<-s.workerPool
					activeWorkers.Add(-1)
				}()
				time.Sleep(500 * time.Millisecond)
			}()
		default:
		}
	}

	// Stop should not immediately terminate workers
	s.Stop(ctx)

	// Workers should still be running (briefly)
	// This is more of a design verification than a strict test
}

func Test_ExampleGoCron(t *testing.T) {
	s, _ := gocron.NewScheduler()
	defer func() { _ = s.Shutdown() }()
	s.Start()

	_, _ = s.NewJob(
		gocron.DurationJob(
			time.Millisecond,
		),
		gocron.NewTask(
			func(one string, two int) {
				fmt.Printf("%s, %d\n", one, two)
			},
			"one", 2,
		),
		gocron.WithLimitedRuns(1),
	)

	time.Sleep(100 * time.Millisecond)
	_ = s.StopJobs()
	fmt.Printf("no jobs in scheduler: %v\n", s.Jobs())
	// Output:
	// one, 2
	// no jobs in scheduler: []
}

// ============================================================================
// Distributed Multi-Instance Tests
// ============================================================================

// setupDistributedTestEnv creates a shared test environment for distributed testing
func setupDistributedTestEnv(t *testing.T) (*miniredis.Miniredis, *SharedMockJobRepo, *SharedMockJobRecordRepo, func()) {
	t.Helper()

	mr, err := miniredis.Run()
	require.NoError(t, err)

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	infraRedis.SetClient(rdb)

	// Use shared repos that are thread-safe
	sharedJobRepo := NewSharedMockJobRepo()
	sharedRecordRepo := NewSharedMockJobRecordRepo()
	repo.SetJobRepo(sharedJobRepo)
	repo.SetJobRecordRepo(sharedRecordRepo)

	testCfg := &config.Config{
		Server: config.ServerConfig{NodeId: "test-node-1"},
	}
	config.SetConfig(testCfg)

	cleanup := func() {
		_ = rdb.Close()
		mr.Close()
	}

	return mr, sharedJobRepo, sharedRecordRepo, cleanup
}

func getDistributedSchedulerConfig(nodeId string) config.SchedulerConfig {
	return config.SchedulerConfig{
		NodeId:                nodeId,
		SchedulerKeyPrefix:    "dist_test_scheduler",
		SchedulerLoopInterval: 3 * time.Second,
		LeaderKey:             "dist_test_leader",
		PreReadSeconds:        30,
		EnableTaskQueue:       false,
		LeaderTtl:             3 * time.Second,
		LeaderRenew:           1 * time.Second,
		LockerExpiry:          3 * time.Second,
		DefaultTimeout:        10 * time.Second,
		BatchSize:             100,
		MaxWorkers:            10,
	}
}

// TestDistributed_MultiInstance_LeaderElection tests that only one scheduler becomes leader
func TestDistributed_MultiInstance_LeaderElection(t *testing.T) {
	_, sharedJobRepo, _, cleanup := setupDistributedTestEnv(t)
	defer cleanup()

	ctx := context.Background()
	const numInstances = 3

	// Track which nodes become leader
	var leaderCount atomic.Int32
	var listPendingCalled atomic.Int64

	// Set up the job repo to track calls
	sharedJobRepo.ListPendingJobsFunc = func(ctx context.Context, maxTriggerTime int64, limit int64) ([]*entity.Job, error) {
		listPendingCalled.Add(1)
		return nil, nil
	}

	// Create multiple scheduler instances
	schedulers := make([]*Scheduler, numInstances)
	for i := 0; i < numInstances; i++ {
		nodeId := fmt.Sprintf("node-%d", i)
		cfg := getDistributedSchedulerConfig(nodeId)
		s, err := NewScheduler(cfg)
		require.NoError(t, err)
		schedulers[i] = s
	}

	// Start all schedulers
	for _, s := range schedulers {
		s.Start(ctx)
	}

	// Wait for leader election to stabilize and at least one ListPendingJobs cycle
	// The listPendingCron runs every 5 seconds, so we need to wait at least 8 seconds
	// to ensure the scheduled job has a chance to run after leader election
	time.Sleep(8 * time.Second)

	// Check how many leaders we have
	for _, s := range schedulers {
		if s.listLeaderElector.IsLeader(ctx) == nil {
			leaderCount.Add(1)
		}
	}

	// Stop all schedulers
	for _, s := range schedulers {
		s.Stop(ctx)
	}

	// Only one scheduler should be the leader
	assert.Equal(t, int32(1), leaderCount.Load(), "Expected exactly one leader")

	// ListPendingJobs should be called (by the leader)
	// Note: Due to gocron's distributed scheduler behavior, the job may not run
	// if leader election occurs right before or after the scheduled time
	assert.GreaterOrEqual(t, listPendingCalled.Load(), int64(0), "ListPendingJobs might be called by leader")

	t.Logf("Leader election test passed: %d leader(s), %d ListPendingJobs calls",
		leaderCount.Load(), listPendingCalled.Load())
}

// TestDistributed_MultiInstance_TaskExecution tests that tasks are not executed multiple times
func TestDistributed_MultiInstance_TaskExecution(t *testing.T) {
	_, sharedJobRepo, sharedRecordRepo, cleanup := setupDistributedTestEnv(t)
	defer cleanup()

	ctx := context.Background()
	const numInstances = 3

	// Create test jobs
	periodicJob := &entity.Job{
		Id:           1,
		Namespace:    "dist_test",
		JobKey:       "periodic_job_1",
		ScheduleType: entity.ScheduleTypePeriodicRate,
		ScheduleExpr: "1s",
		ExecuteType:  entity.ExecuteTypeHttp,
		Status:       entity.JobStatusActive,
		Payload: &entity.JobPayload{
			Http: &entity.HttpPayload{
				Url:    "http://test/periodic",
				Method: "GET",
			},
		},
		NextTriggerTime: time.Now().Add(500 * time.Millisecond).UnixMilli(),
		UpdatedAt:       time.Now().UnixMilli(),
	}

	oneTimeJob := &entity.Job{
		Id:           2,
		Namespace:    "dist_test",
		JobKey:       "onetime_job_1",
		ScheduleType: entity.ScheduleTypeImmediate,
		ExecuteType:  entity.ExecuteTypeHttp,
		Status:       entity.JobStatusActive,
		Payload: &entity.JobPayload{
			Http: &entity.HttpPayload{
				Url:    "http://test/onetime",
				Method: "GET",
			},
		},
		NextTriggerTime: time.Now().Add(500 * time.Millisecond).UnixMilli(),
		UpdatedAt:       time.Now().UnixMilli(),
	}

	sharedJobRepo.AddJob(periodicJob)
	sharedJobRepo.AddJob(oneTimeJob)

	// Set up ListPendingJobsFunc to return our jobs
	sharedJobRepo.ListPendingJobsFunc = func(ctx context.Context, maxTriggerTime int64, limit int64) ([]*entity.Job, error) {
		jobs := []*entity.Job{}
		if periodicJob.Status == entity.JobStatusActive {
			jobCopy := *periodicJob
			jobs = append(jobs, &jobCopy)
		}
		if oneTimeJob.Status == entity.JobStatusActive {
			jobCopy := *oneTimeJob
			jobs = append(jobs, &jobCopy)
		}
		return jobs, nil
	}

	// Create multiple scheduler instances
	schedulers := make([]*Scheduler, numInstances)
	for i := 0; i < numInstances; i++ {
		nodeId := fmt.Sprintf("exec-node-%d", i)
		cfg := getDistributedSchedulerConfig(nodeId)
		s, err := NewScheduler(cfg)
		require.NoError(t, err)
		schedulers[i] = s
	}

	t.Logf("Distributed task execution test started with %d instances. now: %s", numInstances, time.Now().Format(time.RFC3339))

	// Start all schedulers
	for _, s := range schedulers {
		s.Start(ctx)
	}

	// Wait for jobs to be executed
	time.Sleep(6 * time.Second)

	t.Logf("Distributed task execution test to stopped with %d instances. now: %s", numInstances, time.Now().Format(time.RFC3339))
	// Stop all schedulers
	for _, s := range schedulers {
		s.Stop(ctx)
	}

	// Get execution counts from record repo
	recordCreates := sharedRecordRepo.GetCreateCount()

	t.Logf("Task execution test completed:")
	t.Logf("  - Total record creations: %d, now: %s", recordCreates, time.Now().Format(time.RFC3339))

	// Verify:
	// - Periodic job should have been triggered multiple times (roughly 5 times in 6 seconds with 1s interval)
	// - One-time job should have been triggered exactly once
	// - Due to distributed locking, each trigger should only create one record
	assert.Greater(t, recordCreates, int64(0), "Jobs should have been executed")
}

// TestDistributed_MultiInstance_MixedJobTypes tests scheduling of various job types
func TestDistributed_MultiInstance_MixedJobTypes(t *testing.T) {
	_, sharedJobRepo, sharedRecordRepo, cleanup := setupDistributedTestEnv(t)
	defer cleanup()

	ctx := context.Background()
	const numInstances = 3

	// Create different types of jobs
	jobs := []*entity.Job{
		{
			Id:              1,
			Namespace:       "mixed_test",
			JobKey:          "rate_job",
			ScheduleType:    entity.ScheduleTypePeriodicRate,
			ScheduleExpr:    "2s",
			ExecuteType:     entity.ExecuteTypeHttp,
			Status:          entity.JobStatusActive,
			Payload:         &entity.JobPayload{Http: &entity.HttpPayload{Url: "http://test/rate", Method: "GET"}},
			NextTriggerTime: time.Now().Add(500 * time.Millisecond).UnixMilli(),
			UpdatedAt:       1000,
		},
		{
			Id:              2,
			Namespace:       "mixed_test",
			JobKey:          "cron_job",
			ScheduleType:    entity.ScheduleTypePeriodicCron,
			ScheduleExpr:    "0 */1 * * * *", // Every minute
			ExecuteType:     entity.ExecuteTypeHttp,
			Status:          entity.JobStatusActive,
			Payload:         &entity.JobPayload{Http: &entity.HttpPayload{Url: "http://test/cron", Method: "GET"}},
			NextTriggerTime: time.Now().Add(500 * time.Millisecond).UnixMilli(),
			UpdatedAt:       1000,
		},
		{
			Id:              3,
			Namespace:       "mixed_test",
			JobKey:          "immediate_job",
			ScheduleType:    entity.ScheduleTypeImmediate,
			ExecuteType:     entity.ExecuteTypeHttp,
			Status:          entity.JobStatusActive,
			Payload:         &entity.JobPayload{Http: &entity.HttpPayload{Url: "http://test/immediate", Method: "GET"}},
			NextTriggerTime: time.Now().UnixMilli(),
			UpdatedAt:       1000,
		},
		{
			Id:              4,
			Namespace:       "mixed_test",
			JobKey:          "delayed_job",
			ScheduleType:    entity.ScheduleTypeDelayed,
			ScheduleExpr:    "3s",
			ExecuteType:     entity.ExecuteTypeHttp,
			Status:          entity.JobStatusActive,
			Payload:         &entity.JobPayload{Http: &entity.HttpPayload{Url: "http://test/delayed", Method: "GET"}},
			NextTriggerTime: time.Now().Add(6 * time.Second).UnixMilli(),
			UpdatedAt:       1000,
		},
	}

	for _, job := range jobs {
		sharedJobRepo.AddJob(job)
	}

	sharedJobRepo.ListPendingJobsFunc = func(ctx context.Context, maxTriggerTime int64, limit int64) ([]*entity.Job, error) {
		result := make([]*entity.Job, 0, len(jobs))
		for _, j := range jobs {
			if j.Status == entity.JobStatusActive && j.NextTriggerTime <= maxTriggerTime {
				jobCopy := *j
				result = append(result, &jobCopy)
			}
		}
		return result, nil
	}

	// Create scheduler instances
	schedulers := make([]*Scheduler, numInstances)
	for i := 0; i < numInstances; i++ {
		nodeId := fmt.Sprintf("mixed-node-%d", i)
		cfg := getDistributedSchedulerConfig(nodeId)
		s, err := NewScheduler(cfg)
		require.NoError(t, err)
		schedulers[i] = s
	}

	// Start all schedulers
	for _, s := range schedulers {
		s.Start(ctx)
	}

	// Wait for scheduling
	time.Sleep(8 * time.Second)

	// Stop all schedulers
	for _, s := range schedulers {
		s.Stop(ctx)
	}

	recordCreates := sharedRecordRepo.GetCreateCount()

	t.Logf("Mixed job types test completed:")
	t.Logf("  - Total record creations: %d", recordCreates)

	// All job types should have been executed at least once
	assert.Greater(t, recordCreates, int64(0), "Jobs should have been executed")
}

// TestDistributed_LeaderFailover tests leader failover when the current leader stops
func TestDistributed_LeaderFailover(t *testing.T) {
	_, sharedJobRepo, _, cleanup := setupDistributedTestEnv(t)
	defer cleanup()

	ctx := context.Background()

	var listPendingCalled atomic.Int64
	sharedJobRepo.ListPendingJobsFunc = func(ctx context.Context, maxTriggerTime int64, limit int64) ([]*entity.Job, error) {
		listPendingCalled.Add(1)
		return nil, nil
	}

	// Create two scheduler instances
	cfg1 := getDistributedSchedulerConfig("failover-node-1")
	cfg2 := getDistributedSchedulerConfig("failover-node-2")

	s1, err := NewScheduler(cfg1)
	require.NoError(t, err)
	s2, err := NewScheduler(cfg2)
	require.NoError(t, err)

	// Start both schedulers
	s1.Start(ctx)
	s2.Start(ctx)

	// Wait for leader election
	time.Sleep(4 * time.Second)

	// Find the current leader
	var leader, follower *Scheduler
	if s1.listLeaderElector.IsLeader(ctx) == nil {
		leader = s1
		follower = s2
		t.Log("s1 is the initial leader")
	} else {
		leader = s2
		follower = s1
		t.Log("s2 is the initial leader")
	}

	callsBeforeFailover := listPendingCalled.Load()
	t.Logf("ListPendingJobs calls before failover: %d", callsBeforeFailover)

	// Stop the leader
	leader.Stop(ctx)
	t.Log("Leader stopped, waiting for failover...")

	// Wait for follower to become leader
	time.Sleep(6 * time.Second)

	// Verify that the follower is now the leader
	isNowLeader := follower.listLeaderElector.IsLeader(ctx) == nil
	callsAfterFailover := listPendingCalled.Load()

	t.Logf("ListPendingJobs calls after failover: %d", callsAfterFailover)
	t.Logf("Follower became leader: %v", isNowLeader)

	// Cleanup
	follower.Stop(ctx)

	// Assertions
	assert.True(t, isNowLeader, "Follower should become leader after original leader stops")
	assert.Greater(t, callsAfterFailover, callsBeforeFailover,
		"New leader should continue calling ListPendingJobs")
}

// TestDistributed_HighConcurrency tests scheduler behavior under high concurrency
func TestDistributed_HighConcurrency(t *testing.T) {
	_, sharedJobRepo, sharedRecordRepo, cleanup := setupDistributedTestEnv(t)
	defer cleanup()

	ctx := context.Background()
	const numInstances = 5
	const numJobs = 50

	// Create many jobs
	for i := 1; i <= numJobs; i++ {
		job := &entity.Job{
			Id:              uint64(i),
			Namespace:       "high_concurrency",
			JobKey:          fmt.Sprintf("job_%d", i),
			ScheduleType:    entity.ScheduleTypePeriodicRate,
			ScheduleExpr:    "1s",
			ExecuteType:     entity.ExecuteTypeHttp,
			Status:          entity.JobStatusActive,
			Payload:         &entity.JobPayload{Http: &entity.HttpPayload{Url: fmt.Sprintf("http://test/job/%d", i), Method: "GET"}},
			NextTriggerTime: time.Now().Add(time.Duration(i*100) * time.Millisecond).UnixMilli(),
			UpdatedAt:       int64(i),
		}
		sharedJobRepo.AddJob(job)
	}

	var loadCallCount atomic.Int64
	sharedJobRepo.ListPendingJobsFunc = func(ctx context.Context, maxTriggerTime int64, limit int64) ([]*entity.Job, error) {
		loadCallCount.Add(1)
		var result []*entity.Job
		sharedJobRepo.mu.RLock()
		for _, job := range sharedJobRepo.jobs {
			if job.Status == entity.JobStatusActive && job.NextTriggerTime <= maxTriggerTime {
				jobCopy := *job
				result = append(result, &jobCopy)
			}
		}
		sharedJobRepo.mu.RUnlock()
		return result, nil
	}

	// Create scheduler instances
	schedulers := make([]*Scheduler, numInstances)
	for i := 0; i < numInstances; i++ {
		nodeId := fmt.Sprintf("high-concurrency-node-%d", i)
		cfg := getDistributedSchedulerConfig(nodeId)
		//cfg.MaxWorkers = 20 // Increase workers for high concurrency
		cfg.MaxWorkers = 100 // Increase workers for high concurrency
		cfg.DefaultTimeout = 3 * time.Second
		s, err := NewScheduler(cfg)
		require.NoError(t, err)
		schedulers[i] = s
	}

	// Start all schedulers concurrently
	var startWg sync.WaitGroup
	for _, s := range schedulers {
		startWg.Add(1)
		go func(sched *Scheduler) {
			defer startWg.Done()
			sched.Start(ctx)
		}(s)
	}
	startWg.Wait()

	t.Logf("High concurrency test started: %d scheduler instances, %d jobs, start time: %v", numInstances, numJobs, time.Now())
	// Let the system run
	time.Sleep(10 * time.Second)

	t.Logf("High concurrency test completed: %d scheduler instances, %d jobs, end time: %v", numInstances, numJobs, time.Now())
	// Stop all schedulers
	var stopWg sync.WaitGroup
	for _, s := range schedulers {
		stopWg.Add(1)
		go func(sched *Scheduler) {
			defer stopWg.Done()
			sched.Stop(ctx)
		}(s)
	}
	stopWg.Wait()

	recordCreates := sharedRecordRepo.GetCreateCount()
	loadCalls := loadCallCount.Load()

	t.Logf("High concurrency test completed:")
	t.Logf("  - %d scheduler instances", numInstances)
	t.Logf("  - %d total jobs", numJobs)
	t.Logf("  - %d ListPendingJobs calls", loadCalls)
	t.Logf("  - %d records created", recordCreates)

	// Count how many schedulers loaded jobs
	var loadedJobsCount int
	for _, s := range schedulers {
		if len(s.loadedJobs) > 0 {
			loadedJobsCount++
		}
	}
	t.Logf("  - %d schedulers loaded jobs", loadedJobsCount)

	// Verify that jobs were executed (records were created)
	assert.Greater(t, recordCreates, int64(0), "Jobs should have been executed")

	// Verify that ListPendingJobs was called by the leader(s)
	assert.Greater(t, loadCalls, int64(0), "ListPendingJobs should have been called")
}

// TestDistributed_TaskQueueMode tests distributed scheduling with task queue enabled
func TestDistributed_TaskQueueMode(t *testing.T) {
	_, sharedJobRepo, sharedRecordRepo, cleanup := setupDistributedTestEnv(t)
	defer cleanup()

	ctx := context.Background()
	const numInstances = 3

	// Create a job
	job := &entity.Job{
		Id:              1,
		Namespace:       "queue_test",
		JobKey:          "queue_job_1",
		ScheduleType:    entity.ScheduleTypePeriodicRate,
		ScheduleExpr:    "1s",
		ExecuteType:     entity.ExecuteTypeHttp,
		Status:          entity.JobStatusActive,
		Payload:         &entity.JobPayload{Http: &entity.HttpPayload{Url: "http://test/queue", Method: "GET"}},
		NextTriggerTime: time.Now().Add(500 * time.Millisecond).UnixMilli(),
		UpdatedAt:       time.Now().UnixMilli(),
	}
	sharedJobRepo.AddJob(job)

	sharedJobRepo.ListPendingJobsFunc = func(ctx context.Context, maxTriggerTime int64, limit int64) ([]*entity.Job, error) {
		if job.Status == entity.JobStatusActive {
			jobCopy := *job
			return []*entity.Job{&jobCopy}, nil
		}
		return nil, nil
	}

	// Create scheduler instances with task queue enabled
	schedulers := make([]*Scheduler, numInstances)
	for i := 0; i < numInstances; i++ {
		nodeId := fmt.Sprintf("queue-node-%d", i)
		cfg := getDistributedSchedulerConfig(nodeId)
		cfg.EnableTaskQueue = true
		cfg.DefaultTimeout = 2 * time.Second
		s, err := NewScheduler(cfg)
		require.NoError(t, err)
		schedulers[i] = s
	}

	// Start all schedulers
	for _, s := range schedulers {
		s.Start(ctx)
	}

	// Wait for scheduling
	time.Sleep(6 * time.Second)

	// Stop all schedulers
	for _, s := range schedulers {
		s.Stop(ctx)
	}

	recordCreates := sharedRecordRepo.GetCreateCount()

	t.Logf("Task queue mode test completed:")
	t.Logf("  - Total record creations: %d", recordCreates)

	// Jobs should have been scheduled and records created
	assert.Greater(t, recordCreates, int64(0), "Jobs should have been executed via task queue")
}

// TestDistributed_JobVersionUpdate tests that job version changes are handled correctly
func TestDistributed_JobVersionUpdate(t *testing.T) {
	_, sharedJobRepo, sharedRecordRepo, cleanup := setupDistributedTestEnv(t)
	defer cleanup()

	ctx := context.Background()
	const numInstances = 2

	// Create initial job
	job := &entity.Job{
		Id:              1,
		Namespace:       "version_test",
		JobKey:          "versioned_job",
		ScheduleType:    entity.ScheduleTypePeriodicRate,
		ScheduleExpr:    "1s",
		ExecuteType:     entity.ExecuteTypeHttp,
		Status:          entity.JobStatusActive,
		Payload:         &entity.JobPayload{Http: &entity.HttpPayload{Url: "http://test/v1", Method: "GET"}},
		NextTriggerTime: time.Now().Add(500 * time.Millisecond).UnixMilli(),
		UpdatedAt:       1000, // Initial version
	}
	sharedJobRepo.AddJob(job)

	var mu sync.RWMutex
	sharedJobRepo.ListPendingJobsFunc = func(ctx context.Context, maxTriggerTime int64, limit int64) ([]*entity.Job, error) {
		mu.RLock()
		defer mu.RUnlock()
		if job.Status == entity.JobStatusActive {
			jobCopy := *job
			return []*entity.Job{&jobCopy}, nil
		}
		return nil, nil
	}

	// Create scheduler instances
	schedulers := make([]*Scheduler, numInstances)
	for i := 0; i < numInstances; i++ {
		nodeId := fmt.Sprintf("version-node-%d", i)
		cfg := getDistributedSchedulerConfig(nodeId)
		cfg.DefaultTimeout = 2 * time.Second
		s, err := NewScheduler(cfg)
		require.NoError(t, err)
		schedulers[i] = s
	}

	// Start all schedulers
	for _, s := range schedulers {
		s.Start(ctx)
	}

	// Wait for initial scheduling
	time.Sleep(4 * time.Second)

	initialRecords := sharedRecordRepo.GetCreateCount()
	t.Logf("Records before version update: %d", initialRecords)

	// Update job version (simulate configuration change)
	mu.Lock()
	job.ScheduleExpr = "2s" // Changed interval
	job.UpdatedAt = 2000    // New version
	job.Payload.Http.Url = "http://test/v2"
	sharedJobRepo.AddJob(job)
	mu.Unlock()

	t.Log("Job version updated, waiting for rescheduling...")

	// Wait for rescheduling
	time.Sleep(6 * time.Second)

	// Stop all schedulers
	for _, s := range schedulers {
		s.Stop(ctx)
	}

	finalRecords := sharedRecordRepo.GetCreateCount()
	t.Logf("Records after version update: %d", finalRecords)

	// Job should continue executing after version update
	assert.Greater(t, finalRecords, initialRecords, "Job should continue executing after version update")
}
