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
	Id              uint64        `json:"id"`
	Namespace       string        `json:"namespace"`
	JobKey          string        `json:"job_key"`
	Desc            string        `json:"desc"`
	Status          JobStatus     `json:"status"`
	ScheduleType    ScheduleType  `json:"schedule_type"`
	ScheduleExpr    string        `json:"schedule_expr"`
	ExecuteType     ExecuteType   `json:"execute_type"`
	Payload         *JobPayload   `json:"payload"`
	Extra           *JobExtraInfo `json:"extra"`
	NextTriggerTime int64         `json:"next_trigger_time"`
	CreatedBy       string        `json:"created_by"`
	CreatedAt       int64         `json:"created_at"` // time milli
	UpdatedAt       int64         `json:"updated_at"` // time milli
}

type JobExtraInfo struct {
}

func (j *Job) TableName() string {
	return "job"
}

func (j *Job) UniqueKey() string {
	return j.Namespace + "_" + j.JobKey
}
