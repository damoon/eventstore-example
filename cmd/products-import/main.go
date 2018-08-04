package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"sync"

	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/Shopify/sarama"
	"github.com/damoon/eventstore-example/pb"
	"github.com/golang/protobuf/proto"
)

var (
	brokerList   = kingpin.Flag("brokerList", "List of brokers to connect").Default("kafka:9092").Strings()
	topic        = kingpin.Flag("topic", "Topic name").Default("products").String()
	verbose      = kingpin.Flag("verbose", "Verbosity").Default("false").Bool()
	currentPath  = kingpin.Arg("current", "path to current import file").Required().String()
	previousPath = kingpin.Arg("previous", "path to previous import file").Default("/dev/zero").String()
)

func main() {

	kingpin.Parse()
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(*brokerList, config)
	if err != nil {
		log.Panicf("failed to setup the kafka producer: %s", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Panicf("failed to close the kafka producer: %s", err)
		}
	}()

	f, err := os.Open(*previousPath)
	if err != nil {
		log.Panicf("failed to open previous import file: %s", err)
	}
	previousUUIDs, err := uuids(f)
	if err != nil {
		log.Panicf("failed to load UUIDs from previous import file: %s", err)
	}

	f, err = os.Open(*currentPath)
	if err != nil {
		log.Panicf("failed to open latest import file: %s", err)
	}

	var wg sync.WaitGroup

	r := csv.NewReader(f)
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}

		delete(previousUUIDs, row[0])

		wg.Add(1)
		go func(row []string) {
			defer wg.Done()
			msg, err := updateMsg(row)
			if err != nil {
				log.Panicf("failed encode message: %s", err)
			}
			send(producer, msg, *verbose)
		}(row)
	}

	for uuid := range previousUUIDs {
		wg.Add(1)
		go func(uuid string) {
			defer wg.Done()
			msg := deleteMsg(uuid)
			send(producer, msg, *verbose)
		}(uuid)
	}

	wg.Wait()
}

func uuids(f io.Reader) (map[string]bool, error) {
	m := make(map[string]bool)
	r := csv.NewReader(f)
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		m[row[0]] = true
	}
	return m, nil
}

func send(producer sarama.SyncProducer, msg *sarama.ProducerMessage, verbose bool) {
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Panicf("failed to send product to kafka: %s", err)
	}
	if verbose {
		fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", *topic, partition, offset)
	}
}

func deleteMsg(uuid string) *sarama.ProducerMessage {
	return &sarama.ProducerMessage{
		Topic: *topic,
		Key:   sarama.StringEncoder(uuid),
		Value: nil,
	}
}

func updateMsg(row []string) (*sarama.ProducerMessage, error) {
	product, err := row2product(row)
	if err != nil {
		return &sarama.ProducerMessage{}, fmt.Errorf("failed to map row to proto buffer product: %s", err)
	}

	bytes, err := proto.Marshal(product)
	if err != nil {
		return &sarama.ProducerMessage{}, fmt.Errorf("failed to serialize product %s: %s", product.GetUuid(), err)
	}

	return &sarama.ProducerMessage{
		Topic: *topic,
		Key:   sarama.StringEncoder(product.GetUuid()),
		Value: sarama.ByteEncoder(bytes),
	}, nil
}

func row2product(row []string) (*pb.Product, error) {
	price, err := strconv.ParseFloat(row[6], 32)
	if err != nil {
		return &pb.Product{}, err
	}
	return &pb.Product{
		Uuid:          row[0],
		Title:         row[1],
		Description:   row[2],
		Longtext:      row[3],
		SmallImageURL: row[4],
		LargeImageURL: row[5],
		Price:         float32(price),
	}, nil
}
