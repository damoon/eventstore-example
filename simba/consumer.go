package simba

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

const msgBuffer = 65536
const maxOffsetDelay = 5 * time.Second

type MaterializedViews interface {
	Incorporate(msg *sarama.ConsumerMessage) error
}

type Consumer struct {
	doneCh    chan struct{}
	partition *cluster.Consumer
	view      MaterializedViews
	msgs      chan *sarama.ConsumerMessage
	wg        *sync.WaitGroup
}

func NewConsumer(consumer *cluster.Consumer, view MaterializedViews) *Consumer {
	return &Consumer{
		partition: consumer,
		doneCh:    make(chan struct{}),
		view:      view,
		msgs:      make(chan *sarama.ConsumerMessage, msgBuffer),
		wg:        &sync.WaitGroup{},
	}
}

func (c *Consumer) Start() {

	OSSignals := make(chan os.Signal, 1)
	signal.Notify(OSSignals, os.Interrupt)

	saveOffset := time.NewTimer(5 * time.Second)

	for {
		select {
		case err := <-c.partition.Errors():
			log.Panicf("failure from kafka consumer: %s", err)

		case ntf := <-c.partition.Notifications():
			log.Printf("Rebalanced: %+v\n", ntf)

		case msg := <-c.partition.Messages():
			c.wg.Add(1)
			c.msgs <- msg
			go func() {
				defer c.wg.Done()
				err := c.view.Incorporate(msg)
				if err != nil {
					log.Panicf("failed to incorporate msg into view: %s", err)
				}
			}()
			if len(c.msgs) == msgBuffer {
				saveOffset.Stop()
				c.persistOffset()
				saveOffset = time.NewTimer(maxOffsetDelay)
			}

		case <-saveOffset.C:
			c.persistOffset()
			saveOffset = time.NewTimer(maxOffsetDelay)

		case <-OSSignals:
			log.Print("interrupt is detected")
			saveOffset.Stop()
			c.persistOffset()
			c.partition.Close()
			return
		}
	}
}

func (c *Consumer) persistOffset() {
	defer func() {}()
	if len(c.msgs) == 0 {
		return
	}
	c.wg.Wait()
	for len(c.msgs) != 0 {
		c.partition.MarkOffset(<-c.msgs, "")
	}
}
