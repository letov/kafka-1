package storage

import (
	"context"
	"fmt"
	"kafka-1/internal/infrastructure/config"

	"github.com/redis/go-redis/v9"
)

type Redis struct {
	client *redis.Client
}

func (r Redis) SaveSuccess(ctx context.Context, key string, val []byte) error {
	return r.client.Set(ctx, fmt.Sprintf("success:%v", key), val, 0).Err()
}

func (r Redis) SaveError(ctx context.Context, key string, val []byte) error {
	return r.client.Set(ctx, fmt.Sprintf("error:%v", key), val, 0).Err()
}

func NewStorage(
	conf *config.Config,
) *Redis {
	opt, _ := redis.ParseURL(conf.RedisOpt)
	client := redis.NewClient(opt)
	return &Redis{client}
}
