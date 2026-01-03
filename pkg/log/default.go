package log

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"time"
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

func (d *defaultLogger) SetLevel(lv slog.Level) {
	d.level = lv
	d.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level:     lv,
		AddSource: true,
	}))
}

func (d *defaultLogger) SetOutput(w io.Writer) {
	d.Logger = slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{
		Level:     d.level,
		AddSource: true,
	}))
}

func (d *defaultLogger) Debug(format string, v ...any) {
	d.log(context.Background(), slog.LevelDebug, format, v...)
}

func (d *defaultLogger) Info(format string, v ...any) {
	d.log(context.Background(), slog.LevelInfo, format, v...)
}

func (d *defaultLogger) Warn(format string, v ...any) {
	d.log(context.Background(), slog.LevelWarn, format, v...)
}

func (d *defaultLogger) Error(format string, v ...any) {
	d.log(context.Background(), slog.LevelError, format, v...)
}

func (d *defaultLogger) CtxDebug(ctx context.Context, format string, v ...any) {
	d.log(ctx, slog.LevelDebug, format, v...)
}

func (d *defaultLogger) CtxInfo(ctx context.Context, format string, v ...any) {
	d.log(ctx, slog.LevelInfo, format, v...)
}

func (d *defaultLogger) CtxWarn(ctx context.Context, format string, v ...any) {
	d.log(ctx, slog.LevelWarn, format, v...)
}

func (d *defaultLogger) CtxError(ctx context.Context, format string, v ...any) {
	d.log(ctx, slog.LevelError, format, v...)
}

// log 是内部方法，用于统一处理日志记录
// skip=3 是因为调用栈是：TestLogger -> Info -> defaultLogger.Info -> log
func (d *defaultLogger) log(ctx context.Context, level slog.Level, format string, v ...any) {
	if !d.Logger.Enabled(ctx, level) {
		return
	}

	var pcs [1]uintptr
	// skip=3: runtime.Callers -> log -> Info/CtxInfo/etc -> actual caller
	runtime.Callers(4, pcs[:])

	r := slog.NewRecord(time.Now(), level, fmt.Sprintf(format, v...), pcs[0])
	_ = d.Logger.Handler().Handle(ctx, r)
}
