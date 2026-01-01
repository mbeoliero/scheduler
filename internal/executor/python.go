package executor

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"

	"github.com/mbeoliero/scheduler/domain/entity"
)

type PythonExecutor struct{}

func NewPythonExecutor() *PythonExecutor {
	return &PythonExecutor{}
}

func (e *PythonExecutor) Execute(ctx context.Context, config *entity.JobPayload) (*Result, error) {
	if config.Python == nil {
		return nil, fmt.Errorf("python payload is nil")
	}

	pythonConfig := config.Python
	if pythonConfig.Script == "" {
		return nil, fmt.Errorf("python script is empty")
	}

	// 使用系统临时目录，避免相对路径问题
	tmpFile, err := os.CreateTemp("", "scheduler-script-*.py")
	if err != nil {
		return &Result{
			Success: false,
			Error:   fmt.Sprintf("failed to create temp py file: %v", err),
		}, err
	}
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
	}()

	if _, err = tmpFile.WriteString(pythonConfig.Script); err != nil {
		return &Result{
			Success: false,
			Error:   fmt.Sprintf("failed to write temp py file: %v", err),
		}, err
	}

	cmd := exec.CommandContext(ctx, "python3", tmpFile.Name())

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err = cmd.Run()
	output := stdout.String()
	if stderr.Len() > 0 {
		output += "\nSTDERR:\n" + stderr.String()
	}

	if err != nil {
		return &Result{
			Success: false,
			Output:  output,
			Error:   err.Error(),
		}, err
	}

	return &Result{
		Success: true,
		Output:  output,
	}, nil
}
