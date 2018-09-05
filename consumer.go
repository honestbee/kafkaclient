package kafkaclient

import (
	"strconv"
	"strings"

	cluster "github.com/bsm/sarama-cluster"
)

type (
	// Consumer to consume message from kafka
	Consumer struct {
		saramaConsumer *cluster.Consumer
		doneChannel    chan struct{}
		monitorer      Monitorer
		consumerGroup  string
		topics         []string
	}

	// Operation to process the message
	// Returns true means Ack and false means Nack
	Operation func(Message) bool
)

// Messages returns stream of kafka message
func (c *Consumer) Messages() <-chan *Message {
	msgChan := make(chan *Message)
	go func(msgChan chan<- *Message) {
		incCounter(c.monitorer, KafkaConsumerStarted, map[string]string{
			"topic":          strings.Join(c.topics, ","),
			"consumer_group": c.consumerGroup,
		})

		messages := c.saramaConsumer.Messages()
	ConsumeMessageLoop:
		for {
			select {
			case msg := <-messages:
				msgChan <- NewMessageFromSaramaMessage(msg)
				incCounter(c.monitorer, KafkaPartitionMessagesIn, map[string]string{
					"consumer_group": c.consumerGroup,
					"topic":          msg.Topic,
					"partition":      strconv.FormatInt(int64(msg.Partition), 10),
					"offset":         strconv.FormatInt(msg.Offset, 10),
				})
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
	incCounter(c.monitorer, KafkaPartitionMessagesAck, map[string]string{
		"consumer_group": c.consumerGroup,
		"topic":          msg.Topic,
		"partition":      strconv.FormatInt(int64(msg.Partition), 10),
		"offset":         strconv.FormatInt(msg.Offset, 10),
	})
}

// Nack to not acknowledge the message
// Will behave the same as Ack
func (c *Consumer) Nack(msg Message) {
	c.Ack(msg)
	incCounter(c.monitorer, KafkaPartitionMessagesNack, map[string]string{
		"consumer_group": c.consumerGroup,
		"topic":          msg.Topic,
		"partition":      strconv.FormatInt(int64(msg.Partition), 10),
		"offset":         strconv.FormatInt(msg.Offset, 10),
	})
}

// Close to stop consuming message from kafka
func (c *Consumer) Close() {
	close(c.doneChannel)
	c.saramaConsumer.Close()

	incCounter(c.monitorer, KafkaConsumerClosed, map[string]string{
		"topic":          strings.Join(c.topics, ","),
		"consumer_group": c.consumerGroup,
	})
}
