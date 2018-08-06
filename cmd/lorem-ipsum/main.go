package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
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
	consumer, err := cluster.NewConsumer(*brokerList, "lorem-ipsum2", topics, config)
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

	mapKey := "lorem-ipsum"
	searchTerm := "lorem ipsum"

	//	log.Printf("partition %d, offset %d, key %s", msg.Partition, msg.Offset, string(msg.Key))

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
