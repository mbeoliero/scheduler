package repo

import (
	"context"

	"github.com/mbeoliero/scheduler/domain/entity"
)

// JobRepo Job仓储接口
type JobRepo interface {
	// Create 创建任务
	Create(ctx context.Context, job *entity.Job) error

	// Update 更新任务
	Update(ctx context.Context, job *entity.Job) error

	// Delete 删除任务
	Delete(ctx context.Context, id uint64) error

	// FindById 根据ID查询
	FindById(ctx context.Context, id uint64) (*entity.Job, error)

	ListPendingJobs(ctx context.Context, latestTriggerTime int64, limit int64) ([]*entity.Job, error)

	// UpdateStatus 更新任务状态
	UpdateStatus(ctx context.Context, id uint64, status entity.JobStatus) error

	// UpdateNextTriggerTime 更新触发时间
	UpdateNextTriggerTime(ctx context.Context, id uint64, nextTrigger int64) error
}

var jobRepo JobRepo

func SetJobRepo(j JobRepo) {
	jobRepo = j
}

func GetJobRepo() JobRepo {
	return jobRepo
}
