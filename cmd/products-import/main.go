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
	brokerList   = kingpin.Flag("brokerList", "List of brokers to connect").Default("kafka:9092").Strings()
	topic        = kingpin.Flag("topic", "Topic name").Default("products").String()
	verbose      = kingpin.Flag("verbose", "Verbosity").Default("false").Bool()
	currentPath  = kingpin.Arg("current", "path to current import file").Required().String()
	previousPath = kingpin.Arg("previous", "path to previous import file").Default("/dev/null").String()
)

func main() {

	kingpin.Parse()
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Flush.MaxMessages = 500
	producer, err := sarama.NewAsyncProducer(*brokerList, config)
	if err != nil {
		log.Panicf("failed to setup the kafka producer: %s", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Panicf("failed to close the kafka producer: %s", err)
		}
	}()

	go func() {
		for err := range producer.Errors() {
			log.Panicf("failed to send msg (key %s): %s", err.Msg.Key, err.Err)
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

	var i int
	f, err = os.Open(*currentPath)
	if err != nil {
		log.Panicf("failed to open latest import file: %s", err)
	}
	r := csv.NewReader(f)
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}

		delete(previousUUIDs, row[0])

		i++
		if *verbose {
			log.Printf("insert/update product #%d %s\n", i, row[0])
		}
		msg, err := updateMsg(row)
		if err != nil {
			log.Panicf("failed to create update message: %s", err)
		}
		producer.Input() <- msg
	}

	for uuid := range previousUUIDs {
		i++
		if *verbose {
			log.Printf("delete product #%d %s\n", i, uuid)
		}
		msg := deleteMsg(uuid)
		producer.Input() <- msg
	}
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
