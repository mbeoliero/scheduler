package executor

import (
	"context"

	"github.com/mbeoliero/scheduler/domain/entity"
)

type GoExecutor struct{}

func NewGoExecutor() *GoExecutor {
	return &GoExecutor{}
}

func (e *GoExecutor) Execute(ctx context.Context, config *entity.JobPayload) (*Result, error) {
	return nil, ErrNotImplemented
}
