package main

import (
	"context"
	"fmt"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/route"

	"github.com/mbeoliero/scheduler/api"
	"github.com/mbeoliero/scheduler/infra/config"
	"github.com/mbeoliero/scheduler/infra/mysql"
	"github.com/mbeoliero/scheduler/infra/redis"
	"github.com/mbeoliero/scheduler/internal/scheduler"
	"github.com/mbeoliero/scheduler/pkg/log"
)

func main() {
	cfg, err := config.Load("config/config.yaml")
	if err != nil {
		panic(fmt.Sprintf("failed to load config: %v", err))
	}

	ctx := context.TODO()

	if err = mysql.Init(); err != nil {
		log.CtxError(ctx, "failed to init mysql: %v", err)
		panic(err)
	}
	log.CtxInfo(ctx, "mysql initialized")

	if err = redis.Init(); err != nil {
		log.CtxError(ctx, "failed to init redis: %v", err)
		panic(err)
	}
	log.CtxInfo(ctx, "redis initialized")

	sched, err := scheduler.NewScheduler(cfg.Scheduler)
	if err != nil {
		log.CtxError(ctx, "failed to create scheduler: %v", err)
		panic(err)
	}
	sched.Start(ctx)
	log.CtxInfo(ctx, "scheduler started")

	h := server.New(server.WithHostPorts(fmt.Sprintf(":%d", cfg.Server.Port)))
	api.RegisterRoutes(h)
	log.CtxInfo(ctx, "server starting on port %d", cfg.Server.Port)

	closed := []route.CtxCallback{
		sched.Stop,
		func(ctx context.Context) {
			log.CtxInfo(ctx, "start to close mysql and redis")
			_ = mysql.Close()
			_ = redis.Close()
		},
	}
	h.OnShutdown = append(h.OnShutdown, closed...)

	if err = h.Run(); err != nil {
		log.CtxError(ctx, "server error: %v", err)
		panic(err)
	}

	log.CtxInfo(ctx, "server stopped")
}
