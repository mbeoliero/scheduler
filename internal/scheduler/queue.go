package scheduler

import (
	"context"
	"errors"
	"time"

	"github.com/bytedance/sonic"
	"github.com/redis/go-redis/v9"
)

type TaskMessage struct {
	Namespace   string    `json:"namespace"`
	RecordId    int64     `json:"record_id"`
	JobId       int64     `json:"job_id"`
	JobKey      string    `json:"job_key"`
	TriggerTime int64     `json:"trigger_time"`
	CreatedAt   time.Time `json:"created_at"`
}

type TaskQueue struct {
	rdb       redis.UniversalClient
	keyPrefix string
}

func NewTaskQueue(rdb redis.UniversalClient, keyPrefix string) *TaskQueue {
	return &TaskQueue{
		rdb:       rdb,
		keyPrefix: keyPrefix,
	}
}

// PushTask 推送任务到队列
func (r *TaskQueue) PushTask(ctx context.Context, msg *TaskMessage) error {
	key := r.keyPrefix + ":task_queue_worker"
	data, _ := sonic.Marshal(msg)
	return r.rdb.LPush(ctx, key, data).Err()
}

// PopTask 从队列获取任务（阻塞）
func (r *TaskQueue) PopTask(ctx context.Context, timeout time.Duration) (*TaskMessage, error) {
	key := r.keyPrefix + ":task_queue_worker"
	result, err := r.rdb.BRPop(ctx, timeout, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}
	if len(result) < 2 {
		return nil, nil
	}

	var msg TaskMessage
	if err = sonic.UnmarshalString(result[1], &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}
