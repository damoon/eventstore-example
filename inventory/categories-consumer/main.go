package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/damoon/eventstore-example/pkg/pb"
	"github.com/damoon/eventstore-example/pkg/simba"
	"github.com/go-redis/redis"
	"github.com/gogo/protobuf/proto"
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
	consumer, err := cluster.NewConsumer(*brokerList, "lorem-ipsum2", topics, config)
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

	mapKey := "lorem-ipsum"
	searchTerm := "lorem ipsum"

	if msg.Value == nil {
		err := redis.SRem(mapKey, string(msg.Key)).Err()
		if err != nil {
			return fmt.Errorf("failed to remove %s from redis set: %s", string(msg.Key), err)
		}
		return nil
	}

	product := &pb.Product{}
	err := proto.Unmarshal(msg.Value, product)
	if err != nil {
		return fmt.Errorf("failed to unmarshal product %s: %s", string(msg.Key), err)
	}
	if !found(product, searchTerm) {
		return nil
	}

	err = redis.SAdd(mapKey, string(msg.Key)).Err()
	if err != nil {
		return fmt.Errorf("failed to add %s to redis set: %s", string(msg.Key), err)
	}
	return nil
}

func found(product *pb.Product, searchTerm string) bool {
	if strings.Contains(product.Title, searchTerm) {
		return true
	}
	if strings.Contains(product.Description, searchTerm) {
		return true
	}
	if strings.Contains(product.Longtext, searchTerm) {
		return true
	}
	return false
}
