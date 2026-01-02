package mysql

import (
	"context"
	"time"

	"gorm.io/gorm"

	"github.com/mbeoliero/scheduler/domain/entity"
	"github.com/mbeoliero/scheduler/domain/repo"
	"github.com/mbeoliero/scheduler/pkg/generic"
)

type jobRecordRepo struct {
	db *gorm.DB
}

var getJobRecordRepo = generic.Once(func() repo.JobRecordRepo {
	return &jobRecordRepo{db: GetDB()}
})

func (j *jobRecordRepo) Create(ctx context.Context, record *entity.JobRecord) error {
	return gorm.G[entity.JobRecord](j.db).Create(ctx, record)
}

func (j *jobRecordRepo) Update(ctx context.Context, record *entity.JobRecord) error {
	_, err := gorm.G[*entity.JobRecord](j.db).Where("id = ?", record.Id).Updates(ctx, record)
	return err
}

func (j *jobRecordRepo) FindById(ctx context.Context, id uint64) (*entity.JobRecord, error) {
	return gorm.G[*entity.JobRecord](j.db).Where("id = ?", id).First(ctx)
}

func (j *jobRecordRepo) UpdateStatus(ctx context.Context, id uint64, status entity.JobRecordStatus, result string) error {
	_, err := gorm.G[*entity.JobRecord](j.db).Where("id = ?", id).Updates(ctx, &entity.JobRecord{
		Id:        id,
		JobStatus: status,
		Result:    result,
		EndTime:   time.Now().UnixMilli(),
	})
	return err
}

func (j *jobRecordRepo) Delete(ctx context.Context, id uint64) error {
	_, err := gorm.G[*entity.JobRecord](j.db).Where("id = ?", id).Delete(ctx)
	return err
}
