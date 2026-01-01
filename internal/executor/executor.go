package executor

import (
	"context"
	"fmt"

	"github.com/mbeoliero/scheduler/domain/entity"
)

var ErrNotImplemented = fmt.Errorf("not implemented")

// Result 执行结果
type Result struct {
	Success bool   `json:"success"`
	Output  string `json:"output"`
	Error   string `json:"error,omitempty"`
}

type Executor interface {
	Execute(ctx context.Context, config *entity.JobPayload) (*Result, error)
}

func GetExecutor(jobType entity.ExecuteType) (Executor, error) {
	switch jobType {
	case entity.ExecuteTypeRpc:
		return NewRpcExecutor(), nil
	case entity.ExecuteTypeHttp:
		return NewHttpExecutor(), nil
	case entity.ExecuteTypeGolang:
		return NewGoExecutor(), nil
	case entity.ExecuteTypePython:
		return NewPythonExecutor(), nil
	case entity.ExecuteTypeShell:
		return NewShellExecutor(), nil
	default:
		return nil, ErrNotImplemented
	}
}
