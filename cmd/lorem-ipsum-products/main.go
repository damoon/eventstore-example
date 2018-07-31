package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	lorem "github.com/drhodes/golorem"
	"github.com/satori/go.uuid"
)

func main() {

	seed := flag.Int64("seed", time.Now().UnixNano(), "seed for random number generator")
	rows := flag.Int("rows", 100000, "number of rows")
	flag.Parse()

	log.Printf("seed: %d\n", *seed)
	rand.Seed(*seed)

	w := csv.NewWriter(os.Stdout)

	for i := 0; i < *rows; i++ {
		UUID, err := uuid.NewV4()
		if err != nil {
			log.Fatalf("failed to generate a UUID: %s\n", err)
			return
		}
		price := float64(rand.Intn(10000)) / 100
		record := []string{
			UUID.String(),
			fmt.Sprintf("%s %s %s", lorem.Word(4, 13), lorem.Word(4, 13), lorem.Word(4, 13)),
			lorem.Sentence(12, 24),
			lorem.Paragraph(3, 6),
			lorem.Url(),
			lorem.Url(),
			fmt.Sprintf("%.2f", price),
		}
		w.Write(record)
	}

	w.Flush()
	if err := w.Error(); err != nil {
		log.Fatalf("error writing csv: %s\n", err)
	}
}
