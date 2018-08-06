package main

import (
	"fmt"
	"log"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/damoon/eventstore-example/simba"
	"github.com/go-redis/redis"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	brokerList    = kingpin.Flag("brokerList", "List of brokers to connect").Default("localhost:9092").Strings()
	topic         = kingpin.Flag("topic", "Topic name").Default("products").String()
	partition     = kingpin.Flag("partition", "Partition number").Default("0").Int32()
	redisAddress  = kingpin.Flag("redisAddress", "Redis Host").Default("redis:6379").String()
	redisPassword = kingpin.Flag("redisPassword", "Redis Password").Default("").String()
	redisDatabase = kingpin.Flag("redisDatabase", "Redis Database").Default("0").Int()
	redisParallel = kingpin.Flag("redisParallel", "Parallel Redis Connections").Default("64").Int64()
)

type productDetails struct {
	simba.Redis
}

func main() {
	kingpin.Parse()

	redis := simba.NewRedis(&redis.Options{
		Addr:     *redisAddress,
		Password: *redisPassword,
		DB:       *redisDatabase,
	})
	view := &productDetails{
		Redis: *redis,
	}

	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Group.Return.Notifications = true
	topics := []string{*topic}
	consumer, err := cluster.NewConsumer(*brokerList, "productdetails2", topics, config)
	if err != nil {
		log.Panicf("failed to setup kafka consumer: %s", err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Panicf("failed to close kafka consumer: %s", err)
		}
	}()

	simba := simba.NewConsumer(consumer, view)
	simba.Start()
}

func (v *productDetails) Incorporate(msg *sarama.ConsumerMessage) error {

	//	log.Printf("partition %d, offset %d, key %s", msg.Partition, msg.Offset, string(msg.Key))

	if msg.Value == nil {
		err := v.Remove(string(msg.Key))
		if err != nil {
			return fmt.Errorf("failed to set %s in redis: %s", string(msg.Key), err)
		}
		return nil
	}

	err := v.Store(string(msg.Key), msg.Value)
	if err != nil {
		return fmt.Errorf("failed to set %s in redis: %s", string(msg.Key), err)
	}
	return nil
}
