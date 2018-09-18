package kafkaclient

import (
	"strconv"
	"strings"

	"github.com/Shopify/sarama"
)

type (
	//go:generate mockery -inpkg -testonly -case underscore -name saramaConsumer
	saramaConsumer interface {
		Messages() <-chan *sarama.ConsumerMessage
		Errors() <-chan error
		MarkOffset(msg *sarama.ConsumerMessage, metadata string)
		Close() error
	}

	// Consumer to consume message from kafka
	Consumer struct {
		saramaConsumer saramaConsumer
		doneChannel    chan struct{}
		monitorer      Monitorer
		logger         Logger
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
		c.logger.Info("Consuming messages.", map[string]interface{}{
			"topic":          c.topics,
			"consumer_group": c.consumerGroup,
		})
	ConsumeMessageLoop:
		for {
			select {
			case msg, ok := <-messages:
				if ok {
					msgChan <- NewMessageFromSaramaMessage(msg)
					incCounter(c.monitorer, KafkaPartitionMessagesIn, map[string]string{
						"consumer_group": c.consumerGroup,
						"topic":          msg.Topic,
						"partition":      strconv.FormatInt(int64(msg.Partition), 10),
						"offset":         strconv.FormatInt(msg.Offset, 10),
					})
				}
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
	c.logger.Info("Acking a message.", map[string]interface{}{
		"topic":          c.topics,
		"consumer_group": c.consumerGroup,
		"partition":      msg.Partition,
		"offset":         msg.Offset,
	})
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
	c.logger.Info("Closing consumer.", map[string]interface{}{
		"consumer_group": c.consumerGroup,
	})
	close(c.doneChannel)
	c.saramaConsumer.Close()

	incCounter(c.monitorer, KafkaConsumerClosed, map[string]string{
		"topic":          strings.Join(c.topics, ","),
		"consumer_group": c.consumerGroup,
	})
}
