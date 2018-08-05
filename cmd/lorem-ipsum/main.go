package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/damoon/eventstore-example/pb"
	"github.com/damoon/eventstore-example/simba"
	"github.com/go-redis/redis"
	"github.com/gogo/protobuf/proto"
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
	}, "loremipsum", *topic, *partition, *redisParallel)
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

	mapKey := "lorem-ipsum"
	searchTerm := "lorem ipsum"

	if msg.Value == nil {
		err := v.SetDel(mapKey, string(msg.Key))
		if err != nil {
			return fmt.Errorf("failed to remove %s from redis set: %s", string(msg.Key), err)
		}
		return nil
	}

	product := &pb.Product{}
	proto.Unmarshal(msg.Value, product)
	if !found(product, searchTerm) {
		return nil
	}

	err := v.SetAdd(mapKey, string(msg.Key))
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
