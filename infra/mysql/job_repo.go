package mysql

import (
	"context"

	"gorm.io/gorm"

	"github.com/mbeoliero/scheduler/domain/entity"
	"github.com/mbeoliero/scheduler/domain/repo"
	"github.com/mbeoliero/scheduler/pkg/generic"
)

type jobRepo struct {
	db *gorm.DB
}

var getJobRepo = generic.Once(func() repo.JobRepo {
	return &jobRepo{db: GetDB()}
})

func (j *jobRepo) Create(ctx context.Context, job *entity.Job) error {
	return gorm.G[entity.Job](j.db).Create(ctx, job)
}

func (j *jobRepo) Update(ctx context.Context, job *entity.Job) error {
	_, err := gorm.G[*entity.Job](j.db).Where("id = ?", job.Id).Updates(ctx, job)
	return err
}

func (j *jobRepo) Delete(ctx context.Context, id uint64) error {
	_, err := gorm.G[entity.Job](j.db).Where("id = ?", id).Delete(ctx)
	return err
}

func (j *jobRepo) FindById(ctx context.Context, id uint64) (*entity.Job, error) {
	return gorm.G[*entity.Job](j.db).Where("id = ?", id).First(ctx)
}

func (j *jobRepo) ListPendingJobs(ctx context.Context, latestTriggerTime int64, limit int64) ([]*entity.Job, error) {
	return gorm.G[*entity.Job](j.db).
		Where("next_trigger_time <= ? AND status = ?", latestTriggerTime, entity.JobStatusActive).
		Order("next_trigger_time").
		Limit(int(limit)).
		Find(ctx)
}

func (j *jobRepo) UpdateStatus(ctx context.Context, id uint64, status entity.JobStatus) error {
	_, err := gorm.G[*entity.Job](j.db).Where("id = ?", id).Update(ctx, entity.FieldStatus, status)
	return err
}

func (j *jobRepo) UpdateNextTriggerTime(ctx context.Context, id uint64, nextTrigger int64) error {
	_, err := gorm.G[*entity.Job](j.db).Where("id = ?", id).Update(ctx, entity.FieldNextTriggerTime, nextTrigger)
	return err
}
