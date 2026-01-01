package executor

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"

	"github.com/mbeoliero/scheduler/domain/entity"
)

type ShellExecutor struct{}

func NewShellExecutor() *ShellExecutor {
	return &ShellExecutor{}
}

func (e *ShellExecutor) Execute(ctx context.Context, config *entity.JobPayload) (*Result, error) {
	if config.Shell == nil {
		return nil, fmt.Errorf("shell payload is nil")
	}

	shellConfig := config.Shell
	var cmd *exec.Cmd

	if shellConfig.Script != "" {
		// 使用系统临时目录，避免相对路径问题
		tmpFile, err := os.CreateTemp("", "scheduler-script-*.sh")
		if err != nil {
			return &Result{
				Success: false,
				Error:   fmt.Sprintf("failed to create temp sh file: %v", err),
			}, err
		}
		defer func() {
			_ = tmpFile.Close()
			_ = os.Remove(tmpFile.Name())
		}()

		if _, err = tmpFile.WriteString(shellConfig.Script); err != nil {
			return &Result{
				Success: false,
				Error:   fmt.Sprintf("failed to write temp sh file: %v", err),
			}, err
		}

		if err = os.Chmod(tmpFile.Name(), 0755); err != nil {
			return &Result{
				Success: false,
				Error:   fmt.Sprintf("failed to chmod script: %v", err),
			}, err
		}

		cmd = exec.CommandContext(ctx, "bash", tmpFile.Name())
	} else if shellConfig.Command != "" {
		cmd = exec.CommandContext(ctx, "bash", "-c", shellConfig.Command)
	} else {
		return nil, fmt.Errorf("both command and script are empty")
	}

	// 捕获输出
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	// 执行命令
	err := cmd.Run()
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
