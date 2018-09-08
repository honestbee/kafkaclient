package kafkaclient

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

type (
	// Counter is the interface for emitting counter type metrics.
	//go:generate mockery -inpkg -testonly -case underscore -name Counter
	Counter interface {
		// Inc increments the counter by a delta.
		Inc(delta int64, tags map[string]string)
	}

	// Monitorer is interface for monitoring
	//go:generate mockery -inpkg -testonly -case underscore -name Monitorer
	Monitorer interface {
		// Counter returns the Counter object corresponding to the name.
		Counter(name string) Counter
	}

	// Client client
	Client struct {
		saramaClient cluster.Client
		config       *Config
		monitorer    Monitorer
	}
)

var (
	ErrConfigIsRequired = errors.New("Config is required")
)

// NewClient returns a new client for to interact with kafka
func NewClient(brokers []string, config *Config, monitorer Monitorer) (*Client, error) {
	if config == nil {
		return nil, ErrConfigIsRequired
	}

	c, err := cluster.NewClient(brokers, &config.Config)
	if err != nil {
		return nil, err
	}

	return &Client{*c, config, monitorer}, nil
}

// NewAsyncProducer returns a new async producer
func (c *Client) NewAsyncProducer() (sarama.AsyncProducer, error) {
	saramaClient := c.saramaClient // copy the value because sarama does not allow reusing client multiple times
	return sarama.NewAsyncProducerFromClient(&saramaClient)
}

// NewConsumer returns a new consumer
func (c *Client) NewConsumer(consumerGroup string, topics []string) (*Consumer, error) {
	saramaClient := c.saramaClient // copy the value because sarama does not allow reusing client multiple times
	consumer, err := cluster.NewConsumerFromClient(&saramaClient, consumerGroup, topics)
	if err != nil {
		return nil, err
	}

	doneChannel := make(chan struct{})

	return &Consumer{consumer, doneChannel, c.monitorer, consumerGroup, topics}, nil
}

// NewRetryableConsumer returns a new retryable consumer
func (c *Client) NewRetryableConsumer(consumerGroup string, topics []string, delayCalculator DelayCalculator, maxAttempt int, operation Operation) (*RetryableConsumer, error) {
	return c.newRetryableConsumer(consumerGroup, topics, delayCalculator, maxAttempt, operation, c.NewConsumer, c.NewAsyncProducer)
}

type (
	newConsumerFunc      func(string, []string) (*Consumer, error)
	newAsyncProducerFunc func() (sarama.AsyncProducer, error)
)

// split into a private method for testing purpose
func (c *Client) newRetryableConsumer(consumerGroup string, topics []string, delayCalculator DelayCalculator, maxAttempt int, operation Operation, createConsumer newConsumerFunc, createAsyncProducer newAsyncProducerFunc) (*RetryableConsumer, error) {
	consumer, err := createConsumer(consumerGroup, topics)
	if err != nil {
		return nil, err
	}

	producer, err := createAsyncProducer()
	if err != nil {
		return nil, err
	}

	firstRetryTopic := ""
	retriers := make([]*RetryableConsumer, maxAttempt)
	for i := 0; i < maxAttempt; i++ {
		retryAttempt := i + 1
		topic := getRetryTopic(topics, retryAttempt)
		if i == 0 {
			firstRetryTopic = topic
		}
		nextRetryTopic := ""
		if retryAttempt >= maxAttempt {
			nextRetryTopic = c.config.DLQTopic
		} else {
			nextRetryTopic = getRetryTopic(topics, retryAttempt+1)
		}

		retryConsumer, err := createConsumer(getRetryConsumerGroup(consumerGroup, retryAttempt), []string{topic})
		if err != nil {
			return nil, err
		}
		retryProducer, err := createAsyncProducer()
		if err != nil {
			return nil, err
		}

		retrier := &RetryableConsumer{
			Consumer:        retryConsumer,
			delayCalculator: delayCalculator,
			attempt:         retryAttempt,
			maxAttempt:      maxAttempt,
			producer:        retryProducer,
			dlqTopic:        c.config.DLQTopic,
			nextRetryTopic:  nextRetryTopic,
		}
		retriers[i] = retrier
	}

	for _, retrier := range retriers {
		go func(retrier *RetryableConsumer) {
			messages := retrier.Messages()
		RetrierConsumeLoop:
			for {
				select {
				case msg := <-messages:
					delay := retrier.delayCalculator.CalculateDelay(retrier.attempt)
					if closed := retrier.sleep(delay); closed {
						break RetrierConsumeLoop
					}
					if succeed := operation(*msg); succeed {
						retrier.Ack(*msg)
					} else {
						retrier.Nack(*msg)
					}
				case <-retrier.doneChannel:
					break RetrierConsumeLoop
				}
			}
		}(retrier)
	}

	return &RetryableConsumer{
		Consumer:        consumer,
		delayCalculator: delayCalculator,
		maxAttempt:      maxAttempt,
		producer:        producer,
		dlqTopic:        c.config.DLQTopic,
		retriers:        retriers,
		nextRetryTopic:  firstRetryTopic,
	}, nil
}

func getRetryTopic(topics []string, attempt int) string {
	return fmt.Sprintf("%s_retry_%d", strings.Join(topics, ","), attempt)
}

func getRetryConsumerGroup(consumerGroup string, attempt int) string {
	return fmt.Sprintf("%s_retry_%d", consumerGroup, attempt)
}
