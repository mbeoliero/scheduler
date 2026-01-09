package entity

const (
	FieldId              = "id"
	FieldNamespace       = "namespace"
	FieldJobKey          = "job_key"
	FieldDesc            = "desc"
	FieldStatus          = "status"
	FieldScheduleType    = "schedule_type"
	FieldScheduleExpr    = "schedule_expr"
	FieldExecuteType     = "execute_type"
	FieldPayload         = "payload"
	FieldExtra           = "extra"
	FieldNextTriggerTime = "next_trigger_time"
	FieldCreatedBy       = "created_by"
	FieldCreatedAt       = "created_at"
	FieldUpdatedAt       = "updated_at"
)

type Job struct {
	Id              uint64        `json:"id" gorm:"primaryKey"`
	Namespace       string        `json:"namespace" gorm:"column:namespace"`
	JobKey          string        `json:"job_key" gorm:"column:job_key"`
	Desc            string        `json:"desc" gorm:"column:desc"`
	Status          JobStatus     `json:"status" gorm:"column:status"`
	ScheduleType    ScheduleType  `json:"schedule_type" gorm:"column:schedule_type"`
	ScheduleExpr    string        `json:"schedule_expr" gorm:"column:schedule_expr"`
	ExecuteType     ExecuteType   `json:"execute_type" gorm:"column:execute_type"`
	Payload         *JobPayload   `json:"payload" gorm:"column:payload;serializer:json"`
	Extra           *JobExtraInfo `json:"extra" gorm:"column:extra;serializer:json"`
	NextTriggerTime int64         `json:"next_trigger_time" gorm:"column:next_trigger_time"`
	CreatedBy       string        `json:"created_by" gorm:"column:created_by"`
	CreatedAt       int64         `json:"created_at" gorm:"column:created_at;autoCreateTime"` // time milli
	UpdatedAt       int64         `json:"updated_at" gorm:"column:updated_at;autoUpdateTime"` // time milli
}

type JobExtraInfo struct {
}

func (j *Job) TableName() string {
	return "job"
}

func (j *Job) UniqueKey() string {
	return j.Namespace + "_" + j.JobKey
}
