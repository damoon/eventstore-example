package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/Shopify/sarama"
	"github.com/go-redis/redis"
)

var (
	brokerList    = kingpin.Flag("brokerList", "List of brokers to connect").Default("localhost:9092").Strings()
	topic         = kingpin.Flag("topic", "Topic name").Default("products").String()
	partition     = kingpin.Flag("partition", "Partition number").Default("0").Int32()
	redisAddress  = kingpin.Flag("redisAddress", "Redis Host").Default("redis:6379").String()
	redisPassword = kingpin.Flag("redisPassword", "Redis Password").Default("").String()
	redisDatabase = kingpin.Flag("redisDatabase", "Redis Database").Default("0").Int()
	redisParallel = kingpin.Flag("redisParallel", "Parallel Redis Connections").Default("64").Int64()
	verbose       = kingpin.Flag("verbose", "Verbosity").Default("false").Bool()
	counter       = kingpin.Flag("counter", "Counter").Default("false").Bool()
)

func main() {
	kingpin.Parse()

	client := redis.NewClient(&redis.Options{
		Addr:     *redisAddress,
		Password: *redisPassword,
		DB:       *redisDatabase,
	})

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	master, err := sarama.NewConsumer(*brokerList, config)
	if err != nil {
		log.Panicf("failed to setup kafka consumer: %s", err)
	}
	defer func() {
		if err := master.Close(); err != nil {
			log.Panicf("failed to close kafka consumer: %s", err)
		}
	}()

	offset, err := fetchOffset(client, int(*partition))
	if err != nil {
		log.Panicf("failed load kafka offset from redis: %v", err)
	}
	log.Printf("fetched stored offset of %d", offset)

	consumer, err := master.ConsumePartition(*topic, *partition, offset)
	if err != nil {
		log.Panicf("failed to select kafka partition: %s", err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	doneCh := make(chan struct{})

	wg := &sync.WaitGroup{}
	sem := semaphore.NewWeighted(*redisParallel)
	i := 0
	var savedOffset int64

	saveOffsetTicker := time.NewTicker(5 * time.Second)
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				log.Panicf("failure from kafka consumer: %s", err)
			case msg := <-consumer.Messages():
				if *counter {
					i++
					log.Printf("#%d", i)
				}
				if *verbose {
					log.Printf("new message with offset %d\n", msg.Offset)
				}
				wg.Add(1)
				if msg.Offset > offset {
					offset = msg.Offset
				}
				go synchronizeDatabase(msg, client, wg, sem)
			case <-saveOffsetTicker.C:
				if savedOffset != offset {
					err = saveOffset(offset, int(*partition), client, sem)
					if err != nil {
						log.Printf("failed to save offset of %d into redis: %s", offset, err)
						continue
					}
					log.Printf("saved offset of %d to redis", offset)
					savedOffset = offset
				}
			case <-signals:
				log.Print("interrupt is detected")
				saveOffsetTicker.Stop()
				wg.Wait()
				doneCh <- struct{}{}
			}
		}
	}()
	<-doneCh

	err = saveOffset(offset, int(*partition), client, sem)
	if err != nil {
		log.Printf("failed to save offset of %d into redis: %s", offset, err)
	}
	log.Printf("saved offset of %d to redis", offset)
}

func fetchOffset(client *redis.Client, partition int) (int64, error) {
	offset, err := client.Get("_kafka_offset_" + strconv.Itoa(partition)).Int64()
	if err == redis.Nil {
		return sarama.OffsetOldest, nil
	}
	if err != nil {
		return 0, err
	}
	return offset, nil
}

func saveOffset(offset int64, partition int, client *redis.Client, sem *semaphore.Weighted) error {
	sem.Acquire(context.Background(), *redisParallel)
	defer sem.Release(*redisParallel)
	return client.Set("_kafka_offset_"+strconv.Itoa(partition), offset, 0).Err()
}

func synchronizeDatabase(msg *sarama.ConsumerMessage, client *redis.Client, wg *sync.WaitGroup, sem *semaphore.Weighted) {
	defer wg.Done()

	if err := sem.Acquire(context.Background(), 1); err != nil {
		log.Panicf("failed to acquire semaphore: %v", err)
		return
	}
	defer sem.Release(1)

	if msg.Value == nil {
		if *verbose {
			log.Printf("delete product %s\n", string(msg.Key))
		}
		err := client.Del(string(msg.Key)).Err()
		if err != nil {
			log.Printf("failed to set %s in redis: %s", string(msg.Key), err)
		}
		return
	}

	if *verbose {
		log.Printf("insert/update product %s\n", string(msg.Key))
	}
	err := client.Set(string(msg.Key), msg.Value, 0).Err()
	if err != nil {
		log.Printf("failed to set %s in redis: %s", string(msg.Key), err)
	}
}
