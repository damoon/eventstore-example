package simba

import (
	"github.com/go-redis/redis"
)

type Redis struct {
	redis redis.Client
}

func NewRedis(options *redis.Options) *Redis {
	return &Redis{
		redis: *redis.NewClient(options),
	}
}
func (r *Redis) Store(key string, value interface{}) error {
	return r.redis.Set(key, value, 0).Err()
}

func (r *Redis) Remove(key string) error {
	return r.redis.Del(key).Err()
}

func (r *Redis) SetAdd(key, value string) error {
	return r.redis.SAdd(key, value).Err()
}

func (r *Redis) SetDel(key, value string) error {
	return r.redis.SRem(key, value).Err()
}
