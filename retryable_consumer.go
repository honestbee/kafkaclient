package kafkaclient

import (
	"strconv"
	"time"

	"github.com/Shopify/sarama"
)

type (
	// DelayCalculator is an interface for calculating delay
	DelayCalculator interface {
		CalculateDelay(numAttempts int) time.Duration
	}

	// RetryableConsumer is a consumer that will do a retry if there is an error on consuming
	RetryableConsumer struct {
		*Consumer
		delayCalculator DelayCalculator
		producer        sarama.AsyncProducer
		attempt         int
		maxAttempt      int
		dlqTopic        string
		nextRetryTopic  string
		retriers        []*RetryableConsumer
	}
)

// Nack to not acknowledge the message and publish to retry topic
func (c *RetryableConsumer) Nack(msg Message) {
	if c.attempt >= c.maxAttempt {
		// publish to dead letter queue
		if c.dlqTopic != "" {
			select {
			case c.producer.Input() <- newSaramaProducerMessage(c.dlqTopic, msg.Key, msg.Value):
				incCounter(c.monitorer, KafkaPublishToDLQ, map[string]string{
					"topic":      c.dlqTopic,
					"from_topic": msg.Topic,
					"attempt":    strconv.FormatInt(int64(c.attempt), 10),
				})
			case <-c.doneChannel:
				return
			}
		}
		c.Ack(msg)
		return
	}

	// publish to retry topic
	if c.nextRetryTopic != "" {
		select {
		case c.producer.Input() <- newSaramaProducerMessage(c.nextRetryTopic, msg.Key, msg.Value):
			incCounter(c.monitorer, KafkaPublishToRetryTopic, map[string]string{
				"topic":      c.nextRetryTopic,
				"from_topic": msg.Topic,
				"attempt":    strconv.FormatInt(int64(c.attempt), 10),
			})
		case <-c.doneChannel:
			return
		}
	}
	c.Ack(msg)
	incCounter(c.monitorer, KafkaPartitionMessagesNack, map[string]string{
		"consumer_group": c.consumerGroup,
		"topic":          msg.Topic,
		"partition":      strconv.FormatInt(int64(msg.Partition), 10),
		"offset":         strconv.FormatInt(msg.Offset, 10),
	})
}

// Close to stop consuming message from kafka
func (c *RetryableConsumer) Close() {
	for _, retrier := range c.retriers {
		retrier.Close()
	}

	c.Consumer.Close()
	c.producer.Close()
}

func (c *RetryableConsumer) sleep(d time.Duration) bool {
	select {
	case <-time.After(d):
		return false
	case <-c.doneChannel:
		return true
	}
}
