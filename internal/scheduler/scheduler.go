package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	redislock "github.com/go-co-op/gocron-redis-lock/v2"
	"github.com/go-co-op/gocron/v2"
	"github.com/go-redsync/redsync/v4"

	"github.com/mbeoliero/scheduler/domain/entity"
	"github.com/mbeoliero/scheduler/domain/repo"
	"github.com/mbeoliero/scheduler/domain/service"
	"github.com/mbeoliero/scheduler/infra/config"
	"github.com/mbeoliero/scheduler/infra/redis"
	"github.com/mbeoliero/scheduler/internal/executor"
	"github.com/mbeoliero/scheduler/pkg/id_gen"
	"github.com/mbeoliero/scheduler/pkg/log"
)

type Scheduler struct {
	cfg               config.SchedulerConfig
	listPendingCron   gocron.Scheduler // 负责从 db 拉取 pending 任务
	listLeaderElector *Elector
	executorCron      gocron.Scheduler // 负责执行任务
	queue             *TaskQueue
	mu                sync.RWMutex
	loadedJobs        map[string]gocron.Job // jobKey -> gocron Job
	jobVersions       map[string]int64      // jobKey -> job.UpdatedAt，用于检查任务是否变更
	stopWorker        chan struct{}
	stopOnce          sync.Once
	started           atomic.Bool
	workerPool        chan struct{} // 协程池信号量，限制并发数
	wg                sync.WaitGroup
}

func NewScheduler(cfg config.SchedulerConfig) (*Scheduler, error) {
	nodeId := cfg.NodeId
	if nodeId == "" {
		nodeId = GetNodeId()
	}
	listLeaderElector, err := NewElector(redis.GetClient(), nodeId, cfg.LeaderKey, WithLockPrefix(cfg.SchedulerKeyPrefix), WithExpiry(cfg.LeaderTtl), WithAutoExtendDuration(cfg.LeaderRenew))
	if err != nil {
		return nil, err
	}
	listPendingCron, err := gocron.NewScheduler(gocron.WithDistributedElector(listLeaderElector))
	if err != nil {
		return nil, err
	}

	locker, err := redislock.NewRedisLockerWithOptions(redis.GetClient(), redislock.WithKeyPrefix(cfg.SchedulerKeyPrefix), redislock.WithRedsyncOptions(redsync.WithExpiry(cfg.LockerExpiry)))
	if err != nil {
		return nil, err
	}
	executorCron, err := gocron.NewScheduler(gocron.WithGlobalJobOptions(gocron.WithDistributedJobLocker(locker)))
	if err != nil {
		return nil, err
	}

	var queue *TaskQueue
	if cfg.EnableTaskQueue {
		queue = NewTaskQueue(redis.GetClient(), cfg.SchedulerKeyPrefix)
	}

	return &Scheduler{
		cfg:               cfg,
		listPendingCron:   listPendingCron,
		listLeaderElector: listLeaderElector,
		executorCron:      executorCron,
		queue:             queue,
		mu:                sync.RWMutex{},
		loadedJobs:        make(map[string]gocron.Job),
		jobVersions:       make(map[string]int64),
		stopWorker:        make(chan struct{}),
		workerPool:        make(chan struct{}, cfg.MaxWorkers), // 初始化协程池
	}, nil
}

func (s *Scheduler) Start(ctx context.Context) {
	s.listPendingCron.Start()
	_ = s.listLeaderElector.Start(ctx)
	s.executorCron.Start()

	// 每五秒执行一次
	job, err := s.listPendingCron.NewJob(
		gocron.DurationJob(s.cfg.SchedulerLoopInterval),
		gocron.NewTask(func() {
			s.loadPendingJobs(ctx)
		}),
	)
	if err != nil {
		log.CtxError(ctx, "failed to new listing pending cron, err: %v", err)
		panic(err)
	}
	log.CtxInfo(ctx, "start scheduler job success, job name: %s", job.Name())

	s.started.Store(true)
	go s.startWorker(ctx)
}

func (s *Scheduler) startWorker(ctx context.Context) {
	if !s.cfg.EnableTaskQueue {
		return
	}

	for {
		select {
		case <-s.stopWorker:
			return
		default:
			s.runTaskWorker(ctx)
		}
	}
}

func (s *Scheduler) loadPendingJobs(ctx context.Context) {
	maxTriggerTime := time.Now().Add(time.Duration(s.cfg.PreReadSeconds) * time.Second).UnixMilli()
	jobs, err := repo.GetJobRepo().ListPendingJobs(ctx, maxTriggerTime, int64(s.cfg.BatchSize))
	if err != nil {
		log.CtxError(ctx, "load pending jobs failed, err: %v", err)
		return
	}

	for _, job := range jobs {
		if err = s.scheduleJob(ctx, job); err != nil {
			log.CtxWarn(ctx, "schedule job failed, jobKey: %s, err: %v", job.UniqueKey(), err)
		}
	}
}

func (s *Scheduler) scheduleJob(ctx context.Context, job *entity.Job) error {
	s.mu.Lock()

	jobKey := job.UniqueKey()
	// 检查任务是否已加载且未变更，避免重复加载
	if loadedJob, exists := s.loadedJobs[jobKey]; exists {
		if oldVersion, ok := s.jobVersions[jobKey]; ok && oldVersion == job.UpdatedAt {
			return nil
		}

		// 任务已变更，移除旧任务
		if err := s.executorCron.RemoveJob(loadedJob.ID()); err != nil {
			log.CtxWarn(ctx, "failed to remove old job, jobKey: %s, err: %v", jobKey, err)
		}
		delete(s.loadedJobs, jobKey)
		delete(s.jobVersions, jobKey)
	}
	s.mu.Unlock()

	var jobDef gocron.JobDefinition
	switch job.ScheduleType {
	case entity.ScheduleTypePeriodicRate:
		dur, err := time.ParseDuration(job.ScheduleExpr)
		if err != nil {
			log.CtxError(ctx, "parse duration failed, jobKey: %s, err: %v", job.JobKey, err)
			return err
		}

		jobDef = gocron.DurationJob(dur)
	case entity.ScheduleTypePeriodicCron:
		jobDef = gocron.CronJob(job.ScheduleExpr, true)
	case entity.ScheduleTypeImmediate, entity.ScheduleTypeDelayed:
		delay := time.Until(time.UnixMilli(job.NextTriggerTime))
		if delay < 0 {
			jobDef = gocron.OneTimeJob(gocron.OneTimeJobStartImmediately())
		} else {
			jobDef = gocron.OneTimeJob(gocron.OneTimeJobStartDateTime(time.Now().Add(delay)))
		}
	default:
		return nil
	}

	jobCopy := *job
	gcJob, err := s.executorCron.NewJob(
		jobDef,
		gocron.NewTask(func() {
			defer func() {
				if r := recover(); r != nil {
					log.CtxError(context.Background(), "job execution panic, jobKey: %s, err: %v", jobCopy.JobKey, r)
				}
			}()

			taskCtx, cancel := context.WithTimeout(context.TODO(), s.cfg.DefaultTimeout)
			defer cancel()

			s.triggerJob(taskCtx, &jobCopy)
		}),
		gocron.WithName(job.UniqueKey()),
	)
	if err != nil {
		log.CtxError(ctx, "schedule job failed, jobKey: %s, err: %v", job.JobKey, err)
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadedJobs[jobKey] = gcJob
	s.jobVersions[jobKey] = job.UpdatedAt
	log.CtxInfo(ctx, "scheduled job successfully, jobKey: %s", job.JobKey)
	return nil
}

func (s *Scheduler) triggerJob(ctx context.Context, job *entity.Job) {
	// 一次性任务触发后从调度器中移除
	if job.ScheduleType != entity.ScheduleTypePeriodicCron && job.ScheduleType != entity.ScheduleTypePeriodicRate {
		s.mu.Lock()
		if loadedJob, exists := s.loadedJobs[job.UniqueKey()]; exists {
			id := loadedJob.ID()
			delete(s.loadedJobs, job.UniqueKey())
			_ = s.executorCron.RemoveJob(id)
		}
		s.mu.Unlock()
	}

	// 创建执行记录
	id, err := id_gen.NextId(ctx)
	if err != nil {
		log.CtxError(ctx, "generate record id failed, err: %v", err)
		return
	}
	record := &entity.JobRecord{
		Id:        uint64(id),
		JobId:     job.Id,
		StartTime: time.Now().UnixMilli(),
		EndTime:   time.Now().UnixMilli(),
		JobStatus: entity.JobRecordStatusPending,
	}
	if s.cfg.EnableTaskQueue {
		if err = repo.GetJobRecordRepo().Create(ctx, record); err != nil {
			log.CtxError(ctx, "create record failed, err: %v", err)
			return
		}

		msg := &TaskMessage{
			RecordId:    int64(record.Id),
			JobId:       int64(job.Id),
			JobKey:      job.JobKey,
			TriggerTime: record.StartTime,
			CreatedAt:   time.Now(),
		}
		if err = s.queue.PushTask(ctx, msg); err != nil {
			log.CtxError(ctx, "push task failed, recordId: %d, jobId: %d, err: %v", record.Id, job.Id, err)
			// 补偿机制：推送失败时将记录状态更新为失败，避免任务丢失
			_ = repo.GetJobRecordRepo().UpdateStatus(ctx, record.Id, entity.JobRecordStatusFailed, fmt.Sprintf("push to queue failed: %v", err))
			return
		}
	} else {
		record.JobStatus = entity.JobRecordStatusRunning
		if err = repo.GetJobRecordRepo().Create(ctx, record); err != nil {
			log.CtxError(ctx, "create record failed, err: %v", err)
			return
		}

		// 使用 workerPool 控制并发，改为异步执行
		select {
		case s.workerPool <- struct{}{}:
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				defer func() { <-s.workerPool }()
				defer func() {
					if r := recover(); r != nil {
						log.CtxError(ctx, "async process task panic, recordId: %d, err: %v", record.Id, r)
						_ = repo.GetJobRecordRepo().UpdateStatus(ctx, record.Id, entity.JobRecordStatusFailed, fmt.Sprintf("panic: %v", r))
					}
				}()
				taskCtx, cancel := context.WithTimeout(context.TODO(), s.cfg.DefaultTimeout)
				defer cancel()

				_ = s.processTask(taskCtx, int64(job.Id), int64(record.Id))
			}()
		case <-s.stopWorker:
			log.CtxWarn(ctx, "scheduler stopped, task skipped. recordId: %d", record.Id)
			_ = repo.GetJobRecordRepo().UpdateStatus(ctx, record.Id, entity.JobRecordStatusFailed, "scheduler stopped")
		}
	}

	log.CtxInfo(ctx, "triggered job, recordId: %d, jobId: %d, jobKey: %s", record.Id, job.Id, job.JobKey)

	s.updateNextTriggerTime(ctx, job)
}

func (s *Scheduler) updateNextTriggerTime(ctx context.Context, job *entity.Job) {
	if !s.started.Load() {
		return
	}

	var nextTime int64
	if job.ScheduleType == entity.ScheduleTypePeriodicCron || job.ScheduleType == entity.ScheduleTypePeriodicRate {
		s.mu.Lock()
		defer s.mu.Unlock()

		eJob, ok := s.loadedJobs[job.UniqueKey()]
		if !ok {
			log.CtxError(ctx, "get job failed, jobKey: %s", job.JobKey)
			return
		}
		nt, err := eJob.NextRun()
		if err != nil {
			if errors.Is(err, gocron.ErrJobNotFound) { // 这个时候可能刚好 scheduler退出了
				log.CtxWarn(ctx, "job not found, jobKey: %s", job.JobKey)
				nt, err = service.CalcNextRunTime(job.ScheduleType, job.ScheduleExpr, time.Now())
				if err != nil {
					log.CtxError(ctx, "calc next run time failed, jobKey: %s, err: %v", job.JobKey, err)
					return
				}
			} else {
				log.CtxError(ctx, "get next run time failed, jobKey: %s, err: %v", job.JobKey, err)
				return
			}
		}

		nextTime = nt.UnixMilli()

		err = repo.GetJobRepo().UpdateNextTriggerTime(ctx, job.Id, nextTime)
		if err != nil {
			log.CtxError(ctx, "update next trigger time failed, jobId: %d, err: %v", job.Id, err)
			return
		}
	} else {
		err := repo.GetJobRepo().UpdateStatus(ctx, job.Id, entity.JobStatusFinished)
		if err != nil {
			log.CtxError(ctx, "failed to update job status, jobId: %d, err: %v", job.Id, err)
		}
	}
}

func (s *Scheduler) runTaskWorker(ctx context.Context) {
	if !s.cfg.EnableTaskQueue {
		return
	}

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	msg, err := s.queue.PopTask(workerCtx, 5*time.Second)
	if err != nil {
		log.CtxError(ctx, "failed to pop task from queue, err: %v", err)
		return
	}
	if msg == nil {
		return
	}

	// 使用协程池限制并发数
	select {
	case s.workerPool <- struct{}{}: // 获取协程池槽位
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer func() { <-s.workerPool }() // 释放槽位
			defer func() {
				if r := recover(); r != nil {
					log.CtxError(ctx, "worker panic processing task, recordId: %d, err: %v", msg.RecordId, r)
					_ = repo.GetJobRecordRepo().UpdateStatus(ctx, uint64(msg.RecordId), entity.JobRecordStatusFailed, fmt.Sprintf("panic: %v", r))
				}
			}()
			s.processTaskFromQueue(ctx, msg)
		}()
	case <-s.stopWorker:
		return
	}
}

func (s *Scheduler) processTaskFromQueue(ctx context.Context, msg *TaskMessage) {
	err := repo.GetJobRecordRepo().UpdateStatus(ctx, uint64(msg.RecordId), entity.JobRecordStatusRunning, "")
	if err != nil {
		log.CtxError(ctx, "update record status failed, recordId: %d, err: %v", msg.RecordId, err)
		return
	}

	log.CtxInfo(ctx, "process task from queue, recordId: %d, jobId: %d, jobKey: %s", msg.RecordId, msg.JobId, msg.JobKey)
	_ = s.processTask(ctx, msg.JobId, msg.RecordId)
}

func (s *Scheduler) processTask(ctx context.Context, jobId, recordId int64) (err error) {
	var result = &executor.Result{}
	defer func() {
		if err != nil {
			if result == nil {
				result = &executor.Result{}
			}
			result.Error = err.Error()
			uErr := repo.GetJobRecordRepo().UpdateStatus(ctx, uint64(recordId), entity.JobRecordStatusFailed, result.Error)
			if uErr != nil {
				log.CtxError(ctx, "update record status failed, recordId: %d, err: %v", recordId, uErr)
			}
		}
	}()

	job, err := repo.GetJobRepo().FindById(ctx, uint64(jobId))
	if err != nil || job == nil {
		log.CtxError(ctx, "get job failed, jobId: %d, err: %v", jobId, err)
		return fmt.Errorf("get job failed, jobId: %d, err: %w", jobId, err)
	}

	timeout := s.cfg.DefaultTimeout
	execCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	start := time.Now()
	exec, err := executor.GetExecutor(job.ExecuteType)
	if err != nil {
		log.CtxError(ctx, "get executor failed, jobId: %d, err: %v", jobId, err)
		return fmt.Errorf("get executor failed, jobId: %d, err: %w", jobId, err)
	}

	result, err = exec.Execute(execCtx, job.Payload)
	log.CtxInfo(ctx, "execute job, recordId: %d, jobId: %d, jobKey: %s, result[%s], err: %v, cost time: %v", recordId, jobId, job.JobKey, result.Log(), err, time.Since(start))
	if err != nil {
		log.CtxError(ctx, "execute job failed, jobId: %d, err: %v", jobId, err)
		return fmt.Errorf("execute job failed, jobId: %d, err: %w", jobId, err)
	}
	if result == nil {
		log.CtxError(ctx, "execute job returned nil result, jobId: %d", jobId)
		return fmt.Errorf("execute job returned nil result, jobId: %d", jobId)
	}

	if result.Success {
		err = repo.GetJobRecordRepo().UpdateStatus(ctx, uint64(recordId), entity.JobRecordStatusSuccess, result.Output)
		if err != nil {
			log.CtxError(ctx, "update record status failed, recordId: %d, err: %v", recordId, err)
			return fmt.Errorf("update record status failed, recordId: %d, err: %w", recordId, err)
		}
	} else {
		// 执行器返回失败（无 error 但 Success=false）
		errMsg := result.Error
		if errMsg == "" {
			errMsg = "executor returned failure without error message"
		}
		uErr := repo.GetJobRecordRepo().UpdateStatus(ctx, uint64(recordId), entity.JobRecordStatusFailed, errMsg)
		if uErr != nil {
			log.CtxError(ctx, "update record status failed, recordId: %d, err: %v", recordId, uErr)
		}
		log.CtxWarn(ctx, "job execution failed, recordId: %d, jobId: %d, error: %s", recordId, jobId, errMsg)
	}
	return nil
}

func (s *Scheduler) Stop(ctx context.Context) {
	if !s.started.Load() {
		return
	}

	s.started.Store(false)
	s.stopOnce.Do(func() {
		close(s.stopWorker)
	})

	_ = s.listPendingCron.Shutdown()
	s.listLeaderElector.Stop()
	_ = s.executorCron.Shutdown()

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	if _, ok := ctx.Deadline(); ok {
		select {
		case <-done:
		case <-ctx.Done():
		}
		return
	}
	<-done
}
