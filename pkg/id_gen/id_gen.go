package id_gen

import (
	"context"
	"time"

	"github.com/sony/sonyflake/v2"
)

var DefaultStartTime = time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

type IdGenerator interface {
	NextId(ctx context.Context) (int64, error)
}

var generator IdGenerator

func SetGenerator(g IdGenerator) {
	generator = g
}

type FlakeIdGenerator struct {
	SF *sonyflake.Sonyflake
}

func (f *FlakeIdGenerator) NextId(ctx context.Context) (int64, error) {
	return f.SF.NextID()
}

func init() {
	sf, err := sonyflake.New(sonyflake.Settings{
		StartTime: DefaultStartTime,
	})
	if err != nil {
		panic(err)
	}
	generator = &FlakeIdGenerator{SF: sf}
}

func NextId(ctx context.Context) (int64, error) {
	return generator.NextId(ctx)
}
