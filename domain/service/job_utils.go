package service

import (
	"fmt"
	"time"

	"github.com/mbeoliero/scheduler/domain/entity"
)

func CalcNextRunTime(scheduleType entity.ScheduleType, scheduleExpr string, now time.Time) (time.Time, error) {
	switch scheduleType {
	case entity.ScheduleTypeImmediate:
		// 立即执行
		return now, nil
	case entity.ScheduleTypeDelayed:
		if len(scheduleExpr) == 0 {
			return now, nil
		}
		// 延迟任务
		dur, err := time.ParseDuration(scheduleExpr)
		if err != nil {
			return time.Time{}, fmt.Errorf("%w: invalid delay duration: %v", ErrInvalidSchedule, err)
		}
		if dur <= 0 {
			return time.Time{}, fmt.Errorf("%w: delay duration must be positive", ErrInvalidSchedule)
		}
		return now.Add(dur), nil
	case entity.ScheduleTypePeriodicCron:
		// 根据 cron 表达式计算下一次触发时间
		schedule, err := cronParser.Parse(scheduleExpr)
		if err != nil {
			return time.Time{}, fmt.Errorf("%w: failed to parse cron expression: %v", ErrInvalidSchedule, err)
		}
		nextTime := schedule.Next(now)
		return nextTime, nil
	case entity.ScheduleTypePeriodicRate:
		dur, err := time.ParseDuration(scheduleExpr)
		if err != nil {
			return time.Time{}, fmt.Errorf("%w: invalid delay duration: %v", ErrInvalidSchedule, err)
		}
		if dur <= 0 {
			return time.Time{}, fmt.Errorf("%w: delay duration must be positive", ErrInvalidSchedule)
		}
		return now.Add(dur), nil
	default:
		return time.Time{}, fmt.Errorf("%w: unknown schedule type: %v", ErrInvalidSchedule, scheduleType)
	}
}
