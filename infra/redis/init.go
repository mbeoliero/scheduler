package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mbeoliero/scheduler/infra/config"
)

var client redis.UniversalClient

func Init() error {
	cfg := config.Get().Redis

	client = redis.NewClient(&redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     100,
		MinIdleConns: 10,
	})

	// 测试连接
	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("failed to connect redis: %w", err)
	}

	return nil
}

func GetClient() redis.UniversalClient {
	return client
}

func Close() error {
	if client != nil {
		return client.Close()
	}
	return nil
}

// SetClient sets the Redis client (used for testing)
func SetClient(c redis.UniversalClient) {
	client = c
}
