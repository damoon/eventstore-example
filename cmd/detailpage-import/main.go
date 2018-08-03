package main

import (
	"fmt"
	"os"
	"os/signal"

	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/Shopify/sarama"
	"github.com/go-redis/redis"
)

var (
	brokerList = kingpin.Flag("brokerList", "List of brokers to connect").Default("localhost:9092").Strings()
	topic      = kingpin.Flag("topic", "Topic name").Default("products").String()
	partition  = kingpin.Flag("partition", "Partition number").Default("0").String()
	offsetType = kingpin.Flag("offsetType", "Offset Type (OffsetNewest | OffsetOldest)").Default("-1").Int()
)

func main() {
	kingpin.Parse()

	client := redis.NewClient(&redis.Options{
		Addr:     "172.17.0.5:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	master, err := sarama.NewConsumer(*brokerList, config)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := master.Close(); err != nil {
			panic(err)
		}
	}()

	consumer, err := master.ConsumePartition(*topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	doneCh := make(chan struct{})

	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				f := set
				if msg.Value == nil {
					f = del
				}
				err := f(client, msg)
				if err != nil {
					fmt.Println(err)
				}
			case <-signals:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()
	<-doneCh
}

func del(client *redis.Client, msg *sarama.ConsumerMessage) error {
	return client.Del(string(msg.Key)).Err()
}

func set(client *redis.Client, msg *sarama.ConsumerMessage) error {
	return client.Set(string(msg.Key), msg.Value, 0).Err()
}