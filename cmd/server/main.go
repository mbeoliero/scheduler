package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/cloudwego/hertz/pkg/app/server"

	"github.com/mbeoliero/scheduler/api"
	"github.com/mbeoliero/scheduler/infra/config"
	"github.com/mbeoliero/scheduler/infra/mysql"
	"github.com/mbeoliero/scheduler/infra/redis"
	"github.com/mbeoliero/scheduler/internal/scheduler"
	"github.com/mbeoliero/scheduler/pkg/log"
)

func main() {
	// 加载配置
	cfg, err := config.Load("config/config.yaml")
	if err != nil {
		panic(fmt.Sprintf("failed to load config: %v", err))
	}

	ctx := context.Background()

	// 初始化 MySQL
	if err := mysql.Init(); err != nil {
		log.CtxError(ctx, "failed to init mysql: %v", err)
		panic(err)
	}
	log.CtxInfo(ctx, "mysql initialized")

	// 初始化 Redis
	if err := redis.Init(); err != nil {
		log.CtxError(ctx, "failed to init redis: %v", err)
		panic(err)
	}
	log.CtxInfo(ctx, "redis initialized")

	// 初始化调度器
	sched, err := scheduler.NewScheduler(cfg.Scheduler)
	if err != nil {
		log.CtxError(ctx, "failed to create scheduler: %v", err)
		panic(err)
	}
	sched.Start(ctx)
	log.CtxInfo(ctx, "scheduler started")

	// 创建 Hertz 服务器
	h := server.Default(server.WithHostPorts(fmt.Sprintf(":%d", cfg.Server.Port)))

	// 注册路由
	api.RegisterRoutes(h)

	log.CtxInfo(ctx, "server starting on port %d", cfg.Server.Port)

	// 启动服务器 (在 goroutine 中启动，以便能够处理关闭信号)
	go func() {
		if err := h.Run(); err != nil {
			log.CtxError(ctx, "server error: %v", err)
			panic(err)
		}
	}()

	// 等待中断信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.CtxInfo(ctx, "shutting down server...")

	// 优雅关闭
	sched.Stop(ctx)
	if err := redis.Close(); err != nil {
		log.CtxError(ctx, "failed to close redis: %v", err)
	}

	log.CtxInfo(ctx, "server stopped")
}
