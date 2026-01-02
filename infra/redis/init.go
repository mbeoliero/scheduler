package redis

import (
	"github.com/redis/go-redis/v9"
)

var client redis.UniversalClient

func Init() error {
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
