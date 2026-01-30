package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/mbeoliero/scheduler/domain/entity"
	"github.com/mbeoliero/scheduler/domain/repo"
	"github.com/mbeoliero/scheduler/pkg/id_gen"
)

var (
	ErrJobNotFound      = errors.New("job not found")
	ErrInvalidSchedule  = errors.New("invalid schedule expression")
	ErrInvalidPayload   = errors.New("invalid payload")
	ErrJobAlreadyExists = errors.New("job already exists")
	ErrInvalidJobStatus = errors.New("invalid job status")

	// cronParser 用于解析和校验 cron 表达式，支持秒级精度（可选）
	cronParser = cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
)

type JobService struct {
	jobRepo       repo.JobRepo
	jobRecordRepo repo.JobRecordRepo
}

func NewJobService() *JobService {
	return &JobService{
		jobRepo:       repo.GetJobRepo(),
		jobRecordRepo: repo.GetJobRecordRepo(),
	}
}

// CreateJob 创建任务
func (s *JobService) CreateJob(ctx context.Context, job *entity.Job) error {
	if err := s.validateJob(job); err != nil {
		return err
	}

	// 生成任务 ID
	id, err := id_gen.NextId(ctx)
	if err != nil {
		return err
	}
	job.Id = uint64(id)

	// 设置初始状态
	job.Status = entity.JobStatusPending
	now := time.Now().UnixMilli()
	job.CreatedAt = now
	job.UpdatedAt = now

	// 计算首次触发时间
	if err := s.calculateNextTriggerTime(job); err != nil {
		return err
	}

	return s.jobRepo.Create(ctx, job)
}

// UpdateJob 更新任务
func (s *JobService) UpdateJob(ctx context.Context, job *entity.Job) error {
	if err := s.validateJob(job); err != nil {
		return err
	}

	// 检查任务是否存在
	existing, err := s.jobRepo.FindById(ctx, job.Id)
	if err != nil {
		return err
	}
	if existing == nil {
		return ErrJobNotFound
	}

	job.UpdatedAt = time.Now().UnixMilli()

	// 重新计算触发时间
	if err := s.calculateNextTriggerTime(job); err != nil {
		return err
	}

	return s.jobRepo.Update(ctx, job)
}

// DeleteJob 删除任务
func (s *JobService) DeleteJob(ctx context.Context, id uint64) error {
	job, err := s.jobRepo.FindById(ctx, id)
	if err != nil {
		return err
	}
	if job == nil {
		return ErrJobNotFound
	}

	return s.jobRepo.Delete(ctx, id)
}

// GetJob 查询任务
func (s *JobService) GetJob(ctx context.Context, id uint64) (*entity.Job, error) {
	job, err := s.jobRepo.FindById(ctx, id)
	if err != nil {
		return nil, err
	}
	if job == nil {
		return nil, ErrJobNotFound
	}
	return job, nil
}

// UpdateJobStatus 更新任务状态
func (s *JobService) UpdateJobStatus(ctx context.Context, id uint64, status entity.JobStatus) error {
	// 检查任务是否存在
	job, err := s.jobRepo.FindById(ctx, id)
	if err != nil {
		return err
	}
	if job == nil {
		return ErrJobNotFound
	}

	return s.jobRepo.UpdateStatus(ctx, id, status)
}

// PauseJob 暂停任务
func (s *JobService) PauseJob(ctx context.Context, id uint64) error {
	return s.UpdateJobStatus(ctx, id, entity.JobStatusPaused)
}

// ResumeJob 恢复任务
func (s *JobService) ResumeJob(ctx context.Context, id uint64) error {
	return s.UpdateJobStatus(ctx, id, entity.JobStatusActive)
}

// GetJobRecord 查询任务执行记录
func (s *JobService) GetJobRecord(ctx context.Context, id uint64) (*entity.JobRecord, error) {
	record, err := s.jobRecordRepo.FindById(ctx, id)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, errors.New("job record not found")
	}
	return record, nil
}

// validateJob 验证任务参数
func (s *JobService) validateJob(job *entity.Job) error {
	if job.Namespace == "" || job.JobKey == "" {
		return errors.New("namespace and job_key are required")
	}

	// 验证 payload
	if job.Payload == nil {
		return ErrInvalidPayload
	}

	switch job.ExecuteType {
	case entity.ExecuteTypeRpc:
		if job.Payload.Rpc == nil {
			return ErrInvalidPayload
		}
	case entity.ExecuteTypeHttp:
		if job.Payload.Http == nil {
			return ErrInvalidPayload
		}
	case entity.ExecuteTypeGolang:
		if job.Payload.Golang == nil {
			return ErrInvalidPayload
		}
	case entity.ExecuteTypePython:
		if job.Payload.Python == nil {
			return ErrInvalidPayload
		}
	case entity.ExecuteTypeShell:
		if job.Payload.Shell == nil {
			return ErrInvalidPayload
		}
	default:
		return errors.New("invalid execute type")
	}

	// 验证调度类型
	switch job.ScheduleType {
	case entity.ScheduleTypeImmediate:
		// 立即执行，无需验证
	case entity.ScheduleTypeDelayed:
		// 延迟执行，使用 duration 表达式（如 "5m", "1h"）
		if job.ScheduleExpr == "" {
			return ErrInvalidSchedule
		}
		dur, err := time.ParseDuration(job.ScheduleExpr)
		if err != nil {
			return fmt.Errorf("%w: invalid delay duration: %w", ErrInvalidSchedule, err)
		}
		if dur <= 0 {
			return fmt.Errorf("%w: delay duration must be positive", ErrInvalidSchedule)
		}
	case entity.ScheduleTypePeriodicCron:
		// Cron 表达式，需要验证表达式合法性
		if job.ScheduleExpr == "" {
			return ErrInvalidSchedule
		}
		// 使用 cron 库校验表达式
		_, err := cronParser.Parse(job.ScheduleExpr)
		if err != nil {
			return fmt.Errorf("%w: invalid cron expression: %w", ErrInvalidSchedule, err)
		}
	case entity.ScheduleTypePeriodicRate:
		// 固定频率，需要验证表达式（如 "1m", "5s"）
		if job.ScheduleExpr == "" {
			return ErrInvalidSchedule
		}
		dur, err := time.ParseDuration(job.ScheduleExpr)
		if err != nil {
			return fmt.Errorf("%w: invalid duration: %w", ErrInvalidSchedule, err)
		}
		// 频率不能小于 1 秒
		if dur < time.Second {
			return fmt.Errorf("%w: rate interval must be at least 1 second", ErrInvalidSchedule)
		}
	default:
		return errors.New("invalid schedule type")
	}

	return nil
}

// calculateNextTriggerTime 计算下次触发时间
func (s *JobService) calculateNextTriggerTime(job *entity.Job) error {
	now := time.Now()
	job.Status = entity.JobStatusActive

	nt, err := CalcNextRunTime(job.ScheduleType, job.ScheduleExpr, now)
	if err != nil {
		return err
	}

	job.NextTriggerTime = nt.UnixMilli()
	return nil
}
