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
	brokerList        = kingpin.Flag("brokerList", "List of brokers to connect").Default("localhost:9092").Strings()
	topic             = kingpin.Flag("topic", "Topic name").Default("products").String()
	partition         = kingpin.Flag("partition", "Partition number").Default("0").String()
	offsetType        = kingpin.Flag("offsetType", "Offset Type (OffsetNewest | OffsetOldest)").Default("-1").Int()
	messageCountStart = kingpin.Flag("messageCountStart", "Message counter start from:").Int()
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
	brokers := *brokerList
	master, err := sarama.NewConsumer(brokers, config)
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
				*messageCountStart++
				//				fmt.Println("Received messages", string(msg.Key), string(msg.Value))
				/*
					product := &pb.Product{}
					err := proto.Unmarshal(msg.Value, product)
					if err != nil {
						fmt.Println(err)
						continue
					}
				*/
				err = client.Set(string(msg.Key), msg.Value, 0).Err()
				if err != nil {
					panic(err)
				}

			case <-signals:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()
	<-doneCh
	fmt.Println("Processed", *messageCountStart, "messages")
}
