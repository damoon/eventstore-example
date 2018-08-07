package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/damoon/eventstore-example/simba"
	"github.com/go-redis/redis"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	brokerList    = kingpin.Flag("brokerList", "List of brokers to connect").Default("localhost:9092").Strings()
	topic         = kingpin.Flag("topic", "Topic name").Default("products").String()
	redisAddress  = kingpin.Flag("redisAddress", "Redis Host").Default("redis:6379").String()
	redisPassword = kingpin.Flag("redisPassword", "Redis Password").Default("").String()
	redisDatabase = kingpin.Flag("redisDatabase", "Redis Database").Default("0").Int()
)

func main() {
	kingpin.Parse()

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

	r := redis.NewClient(&redis.Options{
		Addr:     *redisAddress,
		Password: *redisPassword,
		DB:       *redisDatabase,
	})
	v := func(msg *sarama.ConsumerMessage) error {
		return view(r, msg)
	}
	simba := simba.NewConsumer(consumer, v)
	simba.Start()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	<-signals
	log.Print("interrupt is detected")
	simba.Stop()
}

func view(redis *redis.Client, msg *sarama.ConsumerMessage) error {
	//	log.Printf("partition %d, offset %d, key %s", msg.Partition, msg.Offset, string(msg.Key))

	if msg.Value == nil {
		err := redis.Del(string(msg.Key)).Err()
		if err != nil {
			return fmt.Errorf("failed to set %s in redis: %s", string(msg.Key), err)
		}
		return nil
	}

	err := redis.Set(string(msg.Key), msg.Value, 0).Err()
	if err != nil {
		return fmt.Errorf("failed to set %s in redis: %s", string(msg.Key), err)
	}
	return nil
}
