package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"time"

	lorem "github.com/drhodes/golorem"
	"github.com/satori/go.uuid"
)

func main() {

	seed := flag.Int64("seed", time.Now().UnixNano(), "seed for random number generator")
	add := flag.Int("add", 15, "probability to add a row")
	remove := flag.Int("remove", 10, "probability to remove a row")
	modify := flag.Int("replace", 30, "probability to modify a row")
	flag.Parse()

	log.Printf("seed: %d\n", *seed)
	rand.Seed(*seed)

	r := csv.NewReader(os.Stdin)
	w := csv.NewWriter(os.Stdout)

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}

		if rand.Intn(100) < *add {
			err := w.Write(newRow())
			if err != nil {
				log.Panicf("error adding new row: %s\n", err)
			}
		}
		if rand.Intn(100) < *remove {
			continue
		}
		if rand.Intn(100) < *modify {
			record = modifyRow(record)
		}
		err = w.Write(record)
		if err != nil {
			log.Panicf("error keeping row: %s\n", err)
		}
	}

	w.Flush()
	if err := w.Error(); err != nil {
		log.Fatalf("error writing csv: %s\n", err)
	}
}

func modifyRow(in []string) []string {
	UUID := in[0]
	title := in[1]
	description := in[2]
	longText := in[3]
	smallImageURL := in[4]
	largeImageURL := in[5]
	price := in[6]

	if rand.Intn(2) == 1 {
		title = fmt.Sprintf("%s %s %s", lorem.Word(4, 13), lorem.Word(4, 13), lorem.Word(4, 13))
	}
	if rand.Intn(2) == 1 {
		description = lorem.Sentence(12, 24)
	}
	if rand.Intn(2) == 1 {
		longText = lorem.Paragraph(3, 6)
	}
	if rand.Intn(2) == 1 {
		smallImageURL = lorem.Url()
	}
	if rand.Intn(2) == 1 {
		largeImageURL = lorem.Url()
	}
	if rand.Intn(2) == 1 {
		price = fmt.Sprintf("%.2f", float64(rand.Intn(10000))/100)
	}

	return []string{
		UUID,
		title,
		description,
		longText,
		smallImageURL,
		largeImageURL,
		price,
	}
}

func newRow() []string {
	UUID := uuid.NewV4()
	price := float64(rand.Intn(10000)) / 100
	return []string{
		UUID.String(),
		fmt.Sprintf("%s %s %s", lorem.Word(4, 13), lorem.Word(4, 13), lorem.Word(4, 13)),
		lorem.Sentence(12, 24),
		lorem.Paragraph(3, 6),
		lorem.Url(),
		lorem.Url(),
		fmt.Sprintf("%.2f", price),
	}
}
