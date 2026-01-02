package mysql

import (
	"gorm.io/gorm"

	"github.com/mbeoliero/scheduler/domain/repo"
)

func Init() error {
	repo.SetJobRepo(getJobRepo())
	repo.SetJobRecordRepo(getJobRecordRepo())
	return nil
}

func GetDB() *gorm.DB {
	return nil
}
