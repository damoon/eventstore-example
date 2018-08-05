package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
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

	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Consumer.Return.Errors = true
	kafka, err := sarama.NewConsumer(*brokerList, kafkaConfig)
	if err != nil {
		log.Panicf("failed to setup kafka consumer: %s", err)
	}
	defer func() {
		if err := kafka.Close(); err != nil {
			log.Panicf("failed to close kafka consumer: %s", err)
		}
	}()

	redis := simba.NewRedis(&redis.Options{
		Addr:     *redisAddress,
		Password: *redisPassword,
		DB:       *redisDatabase,
	}, *topic, *partition, *redisParallel)
	view := &productDetails{
		Redis: *redis,
	}
	offset, err := redis.FetchOffset()
	if err != nil {
		log.Panicf("failed load kafka offset from redis: %v", err)
	}
	log.Printf("starting with offset of %d in partition %d", offset, *partition)

	partition, err := kafka.ConsumePartition(*topic, *partition, offset)
	if err != nil {
		log.Panicf("failed to select kafka partition: %s", err)
	}

	consumer := simba.NewConsumer(partition, view)
	consumer.Start()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	<-signals
	log.Print("interrupt is detected")
	consumer.Stop()
}

func (v *productDetails) Incorporate(msg *sarama.ConsumerMessage) error {
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
