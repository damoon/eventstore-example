package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"

	"github.com/Shopify/sarama"
	"github.com/damoon/eventstore-example/pb"
	"github.com/golang/protobuf/proto"
	"github.com/mitchellh/hashstructure"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
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

	previousUUIDs, err := prevUUIDs(*previousPath)
	if err != nil {
		log.Panicf("failed to load UUIDs from previous import file: %s", err)
	}
	previousHashes, err := prevHashes(*previousPath)
	if err != nil {
		log.Panicf("failed to calculate hashes from previous import file: %s", err)
	}

	var rowNumber int
	f, err := os.Open(*currentPath)
	if err != nil {
		log.Panicf("failed to open latest import file: %s", err)
	}
	r := csv.NewReader(f)
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}

		rowNumber++
		UUID := row[0]

		hash, err := hashstructure.Hash(row, nil)
		if err != nil {
			log.Panicf("failed to create update message: %s", err)
		}

		if _, ok := previousHashes[hash]; ok {
			if *verbose {
				log.Printf("skip unchanged product #%d %s\n", rowNumber, UUID)
			}
			continue
		}

		if *verbose {
			ll := "insert product #%d %s\n"
			if _, ok := previousUUIDs[UUID]; ok {
				ll = "update product #%d %s\n"
			}
			log.Printf(ll, rowNumber, UUID)
		}

		msg, err := setMsg(row)
		if err != nil {
			log.Panicf("failed to create update message: %s", err)
		}
		producer.Input() <- msg

		delete(previousUUIDs, UUID)
	}

	for UUID := range previousUUIDs {
		if *verbose {
			log.Printf("delete product %s\n", UUID)
		}
		producer.Input() <- deleteMsg(UUID)
	}
}

func prevUUIDs(path string) (map[string]bool, error) {
	m := make(map[string]bool)

	f, err := os.Open(path)
	if err != nil {
		return m, fmt.Errorf("failed to open previous import file: %s", err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		uuid := row[0]
		m[uuid] = true
	}
	return m, nil
}

func prevHashes(path string) (map[uint64]bool, error) {
	m := make(map[uint64]bool)

	f, err := os.Open(path)
	if err != nil {
		return m, fmt.Errorf("failed to open previous import file: %s", err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		hash, err := hashstructure.Hash(row, nil)
		if err != nil {
			return m, fmt.Errorf("failed to hash product: %s", err)
		}
		m[hash] = true
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

func setMsg(row []string) (*sarama.ProducerMessage, error) {
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
