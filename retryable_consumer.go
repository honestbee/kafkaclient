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
		producer        sarama.SyncProducer
		attempt         int
		maxAttempt      int
		dlqTopic        string
		nextRetryTopic  string
		retriers        []*RetryableConsumer
	}
)

// Nack to not acknowledge the message and publish to retry topic
func (c *RetryableConsumer) Nack(msg Message) {
	// publish to dead letter queue
	if c.attempt >= c.maxAttempt {
		if c.dlqTopic != "" {
			c.logger.Info("Publishing message to DLQ.", map[string]interface{}{
				"from_topic": msg.Topic,
				"partition":  msg.Partition,
				"offset":     msg.Offset,
			})
			_, _, err := c.producer.SendMessage(newSaramaProducerMessage(c.dlqTopic, msg.Key, msg.Value))
			if err != nil {
				c.logger.Error("Fail to publish message to DLQ.", map[string]interface{}{
					"from_topic": msg.Topic,
					"message":    string(msg.Value),
					"error":      err.Error(),
				})
			}
			incCounter(c.monitorer, KafkaPublishToDLQ, map[string]string{
				"topic":      c.dlqTopic,
				"from_topic": msg.Topic,
				"attempt":    strconv.FormatInt(int64(c.attempt), 10),
			})
		}
		c.Ack(msg)
		return
	}

	// publish to retry topic
	if c.nextRetryTopic != "" {
		c.logger.Info("Publishing message to retry topic.", map[string]interface{}{
			"from_topic": msg.Topic,
			"to_topic":   c.nextRetryTopic,
			"partition":  msg.Partition,
			"offset":     msg.Offset,
		})
		_, _, err := c.producer.SendMessage(newSaramaProducerMessage(c.nextRetryTopic, msg.Key, msg.Value))
		if err != nil {
			c.logger.Error("Fail to publish message to retry topic.", map[string]interface{}{
				"from_topic": msg.Topic,
				"to_topic":   c.nextRetryTopic,
				"message":    string(msg.Value),
				"error":      err.Error(),
			})
		}
		incCounter(c.monitorer, KafkaPublishToRetryTopic, map[string]string{
			"topic":      c.nextRetryTopic,
			"from_topic": msg.Topic,
			"attempt":    strconv.FormatInt(int64(c.attempt), 10),
		})
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
