package entity

type ScheduleType int64

const (
	ScheduleTypeImmediate ScheduleType = iota
	ScheduleTypeDelayed
	ScheduleTypePeriodicCron
	ScheduleTypePeriodicRate
)

type ExecuteType int64

const (
	ExecuteTypeRpc ExecuteType = iota
	ExecuteTypeHttp
	ExecuteTypeGolang
	ExecuteTypePython
	ExecuteTypeShell
)

type JobStatus int64

const (
	JobStatusPending JobStatus = iota
	JobStatusActive
	JobStatusFinished
	JobStatusPaused
	JobStatusDisabled
	JobStatusArchived
)

type JobRecordStatus int64

const (
	JobRecordStatusPending JobRecordStatus = iota
	JobRecordStatusRunning
	JobRecordStatusSuccess
	JobRecordStatusFailed
)
