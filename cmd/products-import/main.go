package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"

	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/Shopify/sarama"
	"github.com/damoon/eventstore-example/pb"
	"github.com/golang/protobuf/proto"
)

var (
	brokerList  = kingpin.Flag("brokerList", "List of brokers to connect").Default("kafka:9092").Strings()
	topic       = kingpin.Flag("topic", "Topic name").Default("products").String()
	verbose     = kingpin.Flag("verbose", "Verbosity").Default("false").Bool()
	latestPath  = kingpin.Arg("latest", "path to latest import file").Required().String()
	currentPath = kingpin.Arg("current", "path to current import file").Required().String()
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

	f, err := os.Open(*latestPath)
	if err != nil {
		log.Panicf("failed to open latest import file: %s", err)
	}
	r := csv.NewReader(f)

	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}

		partition, offset, err := send(producer, row)
		if err != nil {
			log.Panicf("failed to send product to kafka: %s", err)
		}
		if *verbose {
			fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", *topic, partition, offset)
		}
	}

}

func send(producer sarama.SyncProducer, row []string) (int32, int64, error) {

	product, err := row2product(row)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to map row to proto buffer product: %s", err)
	}

	bytes, err := proto.Marshal(product)
	if err != nil {
		return 0, 0, err
	}

	msg := &sarama.ProducerMessage{
		Topic: *topic,
		Key:   sarama.StringEncoder(product.GetUuid()),
		Value: sarama.ByteEncoder(bytes),
	}
	return producer.SendMessage(msg)
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
