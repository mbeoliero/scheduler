package log

import (
	"context"
	"io"
	"log/slog"
)

type Logger interface {
	Debug(format string, v ...any)
	Info(format string, v ...any)
	Warn(format string, v ...any)
	Error(format string, v ...any)
}

type CtxLogger interface {
	CtxDebug(ctx context.Context, format string, v ...any)
	CtxInfo(ctx context.Context, format string, v ...any)
	CtxWarn(ctx context.Context, format string, v ...any)
	CtxError(ctx context.Context, format string, v ...any)
}

type Control interface {
	SetLevel(slog.Level)
	SetOutput(io.Writer)
}

type ILogger interface {
	Logger
	CtxLogger
	Control
}
