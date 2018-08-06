package simba

import (
	"log"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

const msgBuffer = 10000
const maxOffsetDelay = 5 * time.Second

type Consumer struct {
	doneCh   chan struct{}
	consumer *cluster.Consumer
	view     func(msg *sarama.ConsumerMessage) error
	msgs     chan *sarama.ConsumerMessage
	inflight *sync.WaitGroup
	marking  *sync.WaitGroup
}

func NewConsumer(consumer *cluster.Consumer, view func(msg *sarama.ConsumerMessage) error) *Consumer {
	return &Consumer{
		consumer: consumer,
		doneCh:   make(chan struct{}),
		view:     view,
		msgs:     make(chan *sarama.ConsumerMessage, msgBuffer),
		inflight: &sync.WaitGroup{},
		marking:  &sync.WaitGroup{},
	}
}

func (c *Consumer) Stop() {
	c.doneCh <- struct{}{}
}

func (c *Consumer) Start() {

	saveOffset := time.NewTimer(5 * time.Second)

	for {
		select {
		case err := <-c.consumer.Errors():
			log.Panicf("failure from kafka consumer: %s", err)

		case ntf := <-c.consumer.Notifications():
			log.Printf("Rebalanced: %+v\n", ntf)

		case msg := <-c.consumer.Messages():
			c.inflight.Add(1)
			c.msgs <- msg
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				err := c.view(msg)
				if err != nil {
					log.Panicf("failed to incorporate msg into view: %s", err)
				}
			}(c.inflight)
			if len(c.msgs) == msgBuffer {
				saveOffset.Stop()
				c.persistOffset()
				saveOffset = time.NewTimer(maxOffsetDelay)
			}

		case <-saveOffset.C:
			c.persistOffset()
			saveOffset = time.NewTimer(maxOffsetDelay)

		case <-c.doneCh:
			log.Print("interrupt is detected")
			saveOffset.Stop()
			c.persistOffset()
			c.consumer.Close()
			return
		}
	}
}

func (c *Consumer) persistOffset() {
	if len(c.msgs) == 0 {
		return
	}

	c.marking.Wait()
	c.marking.Add(1)

	go func(wg *sync.WaitGroup, msgs chan *sarama.ConsumerMessage) {
		var i int64 = 0
		wg.Wait()
		close(msgs)
		for msg := range msgs {
			if i < msg.Offset {
				i = msg.Offset
			}
			c.consumer.MarkOffset(msg, "")
		}
		log.Printf("saved offset %d", i)
		c.marking.Done()
	}(c.inflight, c.msgs)

	c.inflight = &sync.WaitGroup{}
	c.msgs = make(chan *sarama.ConsumerMessage, msgBuffer)
}
