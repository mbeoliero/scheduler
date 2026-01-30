package id_gen

import (
	"context"
	"time"

	"github.com/sony/sonyflake/v2"

	"github.com/mbeoliero/scheduler/pkg/testx"
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
	var sf *sonyflake.Sonyflake
	var err error
	if testx.RunningUnderTest() {
		sf, err = sonyflake.New(sonyflake.Settings{
			StartTime: DefaultStartTime,
			MachineID: func() (int, error) {
				return 1, nil
			},
		})
	} else {
		sf, err = sonyflake.New(sonyflake.Settings{
			StartTime: DefaultStartTime,
		})
	}
	if err != nil {
		panic(err)
	}
	generator = &FlakeIdGenerator{SF: sf}
}

func NextId(ctx context.Context) (int64, error) {
	return generator.NextId(ctx)
}
