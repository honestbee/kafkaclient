package kafkaclient

import (
	cluster "github.com/bsm/sarama-cluster"
)

type (
	// Consumer to consume message from kafka
	Consumer struct {
		saramaConsumer *cluster.Consumer
		doneChannel    chan struct{}
	}

	// Operation to process the message
	// Returns true means Ack and false means Nack
	Operation func(Message) bool
)

// Messages returns stream of kafka message
func (c *Consumer) Messages() <-chan *Message {
	msgChan := make(chan *Message)
	go func(msgChan chan<- *Message) {
		messages := c.saramaConsumer.Messages()
	ConsumeMessageLoop:
		for {
			select {
			case msg := <-messages:
				msgChan <- NewMessageFromSaramaMessage(msg)
			case <-c.doneChannel:
				break ConsumeMessageLoop
			}
		}
	}(msgChan)
	return msgChan
}

// Errors returns stream of error at consuming message
func (c *Consumer) Errors() <-chan error { return c.saramaConsumer.Errors() }

// Ack to acknowledge the message
func (c *Consumer) Ack(msg Message) {
	c.saramaConsumer.MarkOffset(msg.ConsumerMessage, "")
}

// Nack to not acknowledge the message
// Will behave the same as Ack
func (c *Consumer) Nack(msg Message) {
	c.Ack(msg)
}

// Close to stop consuming message from kafka
func (c *Consumer) Close() {
	close(c.doneChannel)
	c.saramaConsumer.Close()
}
