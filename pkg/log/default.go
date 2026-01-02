package log

import (
	"context"
	"io"
	"log/slog"
	"os"
)

var logger ILogger = &defaultLogger{
	level: slog.LevelInfo,
	Logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level:     slog.LevelInfo,
		AddSource: true,
	})),
}

func SetLevel(lv slog.Level) {
	logger.SetLevel(lv)
}

func DefaultLogger() ILogger {
	return logger
}

func SetLogger(v ILogger) {
	logger = v
}

func Info(format string, v ...any) {
	logger.Info(format, v...)
}

func Error(format string, v ...any) {
	logger.Error(format, v...)
}

func Debug(format string, v ...any) {
	logger.Debug(format, v...)
}

func Warn(format string, v ...any) {
	logger.Warn(format, v...)
}

func CtxInfo(ctx context.Context, format string, v ...any) {
	logger.CtxInfo(ctx, format, v...)
}

func CtxDebug(ctx context.Context, format string, v ...any) {
	logger.CtxDebug(ctx, format, v...)
}

func CtxWarn(ctx context.Context, format string, v ...any) {
	logger.CtxWarn(ctx, format, v...)
}

func CtxError(ctx context.Context, format string, v ...any) {
	logger.CtxError(ctx, format, v...)
}

type defaultLogger struct {
	*slog.Logger
	level slog.Level
}

func (d defaultLogger) SetLevel(lv slog.Level) {
	d.level = lv
	d.Logger.Handler().Enabled(context.TODO(), lv)
}

func (d defaultLogger) SetOutput(w io.Writer) {
	d.Logger = slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: d.level}))
}

func (d defaultLogger) CtxDebug(ctx context.Context, format string, v ...any) {
	d.Logger.DebugContext(ctx, format, v...)
}

func (d defaultLogger) CtxInfo(ctx context.Context, format string, v ...any) {
	d.Logger.InfoContext(ctx, format, v...)
}

func (d defaultLogger) CtxWarn(ctx context.Context, format string, v ...any) {
	d.Logger.WarnContext(ctx, format, v...)
}

func (d defaultLogger) CtxError(ctx context.Context, format string, v ...any) {
	d.Logger.ErrorContext(ctx, format, v...)
}
