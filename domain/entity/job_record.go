package entity

type JobRecord struct {
	Id        uint64          `json:"id"`
	JobId     uint64          `json:"job_id"`
	StartTime int64           `json:"start_time"` // time milli
	EndTime   int64           `json:"end_time"`   // time milli
	JobStatus JobRecordStatus `json:"job_status"`
	Result    string          `json:"result"`
	CreatedAt int64           `json:"created_at"` // time milli
	UpdatedAt int64           `json:"updated_at"` // time milli
}

func (j *JobRecord) TableName() string {
	return "job_record"
}
