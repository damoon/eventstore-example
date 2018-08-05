package simba

import (
	"context"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/go-redis/redis"
	"golang.org/x/sync/semaphore"
)

type Redis struct {
	redis       redis.Client
	offsetKey   string
	sem         *semaphore.Weighted
	parallelism int64
}

func NewRedis(options *redis.Options, view, topic string, partition int32, parallelism int64) *Redis {
	return &Redis{
		redis:       *redis.NewClient(options),
		offsetKey:   fmt.Sprintf("_kafka_offset_%s_%s_%d", view, topic, partition),
		sem:         semaphore.NewWeighted(parallelism),
		parallelism: parallelism,
	}
}

func (r *Redis) FetchOffset() (int64, error) {
	err := r.sem.Acquire(context.Background(), 1)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch offset from redis: %s", err)
	}
	defer r.sem.Release(1)
	offset, err := r.redis.Get(r.offsetKey).Int64()
	if err == redis.Nil {
		return sarama.OffsetOldest, nil
	}
	if err != nil {
		return 0, fmt.Errorf("failed to fetch offset from redis: %s", err)
	}
	return offset, nil
}

func (r *Redis) SaveOffset(offset *int64) error {
	err := r.sem.Acquire(context.Background(), r.parallelism)
	if err != nil {
		return fmt.Errorf("failed to save offset of %d to redis: %s", *offset, err)
	}
	defer r.sem.Release(r.parallelism)
	return r.redis.Set(r.offsetKey, *offset, 0).Err()
}

func (r *Redis) Store(key string, value interface{}) error {
	err := r.sem.Acquire(context.Background(), 1)
	if err != nil {
		return fmt.Errorf("failed to store %s to redis: %s", key, err)
	}
	defer r.sem.Release(1)
	return r.redis.Set(key, value, 0).Err()
}

func (r *Redis) Remove(key string) error {
	err := r.sem.Acquire(context.Background(), 1)
	if err == redis.Nil {
		return fmt.Errorf("failed to remove %s from redis: %s", key, err)
	}
	defer r.sem.Release(1)
	return r.redis.Del(key).Err()
}

func (r *Redis) SetAdd(key, value string) error {
	err := r.sem.Acquire(context.Background(), 1)
	if err != nil {
		return fmt.Errorf("failed to store %s to redis: %s", key, err)
	}
	defer r.sem.Release(1)
	return r.redis.SAdd(key, value).Err()
}

func (r *Redis) SetDel(key, value string) error {
	err := r.sem.Acquire(context.Background(), 1)
	if err == redis.Nil {
		return fmt.Errorf("failed to remove %s from redis: %s", key, err)
	}
	defer r.sem.Release(1)
	return r.redis.SRem(key, value).Err()
}
