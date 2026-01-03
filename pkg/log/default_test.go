package log

import (
	"context"
	"testing"
)

func TestLogger(t *testing.T) {
	Info("hello world")
	Info("hello %v", "world")
	CtxInfo(context.TODO(), "hello %v", "world")
}
