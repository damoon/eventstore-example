package simba

import (
	"log"
	"math"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

type MaterializedViews interface {
	SaveOffset(offset *int64) error
	Incorporate(msg *sarama.ConsumerMessage) error
}

type Consumer struct {
	doneCh      chan struct{}
	partition   sarama.PartitionConsumer
	offset      int64
	savedOffset int64
	view        MaterializedViews
}

func NewConsumer(partition sarama.PartitionConsumer, view MaterializedViews) *Consumer {
	return &Consumer{
		doneCh:      make(chan struct{}),
		partition:   partition,
		offset:      math.MinInt64,
		savedOffset: math.MinInt64,
		view:        view,
	}
}

func (c *Consumer) Start() {

	wg := &sync.WaitGroup{}
	saveOffsetTicker := time.NewTicker(5 * time.Second)
	go func() {
		for {
			select {
			case err := <-c.partition.Errors():
				log.Panicf("failure from kafka consumer: %s", err)
			case msg := <-c.partition.Messages():
				wg.Add(1)
				if msg.Offset > c.offset {
					c.offset = msg.Offset
				}
				go func() {
					defer wg.Done()
					err := c.view.Incorporate(msg)
					if err != nil {
						log.Panicf("failed to incorporate msg in partition %d with offset %d into view: %s", msg.Partition, msg.Offset, err)
					}
				}()
			case <-saveOffsetTicker.C:
				c.persistOffset()
			case <-c.doneCh:
				c.partition.Close()
				saveOffsetTicker.Stop()
				wg.Wait()
				c.persistOffset()
			}
		}
	}()
}

func (c *Consumer) Stop() {
	c.doneCh <- struct{}{}
}

func (c *Consumer) persistOffset() {
	if c.savedOffset == c.offset {
		return
	}
	err := c.view.SaveOffset(&c.offset)
	if err != nil {
		log.Printf("failed to save offset of %d: %s", c.offset, err)
		return
	}
	log.Printf("saved offset of %d", c.offset)
	c.savedOffset = c.offset
}
