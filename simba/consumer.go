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
	wg       *sync.WaitGroup
	mux      *sync.Mutex
}

func NewConsumer(consumer *cluster.Consumer, view func(msg *sarama.ConsumerMessage) error) *Consumer {
	return &Consumer{
		consumer: consumer,
		doneCh:   make(chan struct{}),
		view:     view,
		msgs:     make(chan *sarama.ConsumerMessage, msgBuffer),
		wg:       &sync.WaitGroup{},
		mux:      &sync.Mutex{},
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
			c.wg.Add(1)
			c.msgs <- msg
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				err := c.view(msg)
				if err != nil {
					log.Panicf("failed to incorporate msg into view: %s", err)
				}
			}(c.wg)
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

	c.mux.Lock()
	go func(wg *sync.WaitGroup, msgs chan *sarama.ConsumerMessage) {
		defer c.mux.Unlock()
		wg.Wait()
		close(msgs)
		log.Printf("processed %d messages", len(msgs))
		for msg := range msgs {
			c.consumer.MarkOffset(msg, "")
		}
	}(c.wg, c.msgs)

	c.wg = &sync.WaitGroup{}
	c.msgs = make(chan *sarama.ConsumerMessage, msgBuffer)
}
