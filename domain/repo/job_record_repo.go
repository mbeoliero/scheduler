package repo

import (
	"context"

	"github.com/mbeoliero/scheduler/domain/entity"
)

// JobRecordRepo 日志仓储接口
type JobRecordRepo interface {
	// Create 创建日志
	Create(ctx context.Context, record *entity.JobRecord) error

	// Update 更新日志
	Update(ctx context.Context, record *entity.JobRecord) error

	// FindById 根据ID查询
	FindById(ctx context.Context, id uint64) (*entity.JobRecord, error)

	// UpdateStatus 更新日志状态
	UpdateStatus(ctx context.Context, id uint64, status entity.JobRecordStatus, result string) error

	// Delete 删除日志
	Delete(ctx context.Context, id uint64) error
}

var jobRecordRepo JobRecordRepo

func SetJobRecordRepo(j JobRecordRepo) {
	jobRecordRepo = j
}

func GetJobRecordRepo() JobRecordRepo {
	return jobRecordRepo
}
