package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

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
	verbose       = kingpin.Flag("verbose", "Verbosity").Default("false").Bool()
)

func main() {
	kingpin.Parse()

	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Group.Return.Notifications = true
	topics := []string{*topic}
	consumer, err := cluster.NewConsumer(*brokerList, "inventory-categories-v1", topics, config)
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

	p := pb.ProductUpdate{}
	err := proto.Unmarshal(msg.Value, &p)
	if err != nil {
		return fmt.Errorf("failed to unmarshal kafka massaga: %s", err)
	}

	UUID := string(msg.Key)

	if p.New == nil {
		err := redis.SRem(p.Old.Category, UUID).Err()
		if err != nil {
			return fmt.Errorf("failed to remove %s from category %s: %s", UUID, p.Old.Category, err)
		}
		return nil
	}

	if p.Old == nil {
		err := redis.SAdd(p.New.Category, UUID).Err()
		if err != nil {
			return fmt.Errorf("failed to add %s to category %s: %s", UUID, p.New.Category, err)
		}
		return nil
	}

	if p.Old.Category == p.New.Category {
		if *verbose {
			log.Printf("category for %s did not change", UUID)
		}
		return nil
	}

	err = redis.SRem(p.Old.Category, UUID).Err()
	if err != nil {
		return fmt.Errorf("failed to remove %s from category %s: %s", UUID, p.Old.Category, err)
	}
	err = redis.SAdd(p.New.Category, UUID).Err()
	if err != nil {
		return fmt.Errorf("failed to add %s to category %s: %s", UUID, p.New.Category, err)
	}

	return nil
}
