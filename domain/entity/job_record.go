package entity

type JobRecord struct {
	Id        uint64          `json:"id" gorm:"primaryKey"`
	JobId     uint64          `json:"job_id" gorm:"column:job_id"`
	StartTime int64           `json:"start_time" gorm:"column:start_time"` // time milli
	EndTime   int64           `json:"end_time" gorm:"column:end_time"`     // time milli
	JobStatus JobRecordStatus `json:"job_status" gorm:"column:job_status"`
	Result    string          `json:"result" gorm:"column:result"`
	CreatedAt int64           `json:"created_at" gorm:"column:created_at;autoCreateTime"` // time milli
	UpdatedAt int64           `json:"updated_at" gorm:"column:updated_at;autoUpdateTime"` // time milli
}

func (j *JobRecord) TableName() string {
	return "job_record"
}
