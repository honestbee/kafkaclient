package kafkaclient

import (
	"fmt"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/honestbee/kafkaclient/delaycalculator"
	"github.com/honestbee/kafkaclient/testingutil"
	"github.com/stretchr/testify/mock"
)

var monitorer *MockMonitorer

func init() {
	startedCounter := &MockCounter{}
	startedCounter.On("Inc", int64(1), mock.AnythingOfType("map[string]string"))
	closedCounter := &MockCounter{}
	closedCounter.On("Inc", int64(1), mock.AnythingOfType("map[string]string"))
	messageInCounter := &MockCounter{}
	messageInCounter.On("Inc", int64(1), mock.AnythingOfType("map[string]string"))
	messageAckCounter := &MockCounter{}
	messageAckCounter.On("Inc", int64(1), mock.AnythingOfType("map[string]string"))
	messageNackCounter := &MockCounter{}
	messageNackCounter.On("Inc", int64(1), mock.AnythingOfType("map[string]string"))
	retryCounter := &MockCounter{}
	retryCounter.On("Inc", int64(1), mock.AnythingOfType("map[string]string"))
	dlqCounter := &MockCounter{}
	dlqCounter.On("Inc", int64(1), mock.AnythingOfType("map[string]string"))

	monitorer = &MockMonitorer{}
	monitorer.On("Counter", mock.AnythingOfType("string")).Return(func(s string) Counter {
		switch s {
		case KafkaConsumerStarted:
			return startedCounter
		case KafkaConsumerClosed:
			return closedCounter
		case KafkaPartitionMessagesIn:
			return messageInCounter
		case KafkaPartitionMessagesAck:
			return messageAckCounter
		case KafkaPartitionMessagesNack:
			return messageNackCounter
		case KafkaPublishToRetryTopic:
			return retryCounter
		case KafkaPublishToDLQ:
			return dlqCounter
		}

		return startedCounter
	})
}

func newTestConsumer(consumerGroup string, topics []string) (*Consumer, error) {
	consumer := &Consumer{
		saramaConsumer: &mockSaramaConsumer{},
		doneChannel:    make(chan struct{}),
		monitorer:      monitorer,
		consumerGroup:  consumerGroup,
		topics:         topics,
	}

	return consumer, nil
}

func newTestAsyncProducer() (sarama.AsyncProducer, error) {
	producer := &MockAsyncProducer{}

	return producer, nil
}

func createTestRetryableConsumer(t *testing.T, maxAttempt int) *RetryableConsumer {
	consumer, err := newTestConsumer("my_group", []string{"my_topic"})
	testingutil.Ok(t, err)

	producer, err := newTestAsyncProducer()
	testingutil.Ok(t, err)

	calc := delaycalculator.NewLinearDelayCalculator(0 * time.Second)

	retriers := make([]*RetryableConsumer, maxAttempt)
	for i := 0; i < maxAttempt; i++ {
		consumer, err := newTestConsumer("my_group", []string{"my_topic"})
		testingutil.Ok(t, err)

		producer, err := newTestAsyncProducer()
		testingutil.Ok(t, err)

		retriers[i] = &RetryableConsumer{
			Consumer:        consumer,
			delayCalculator: calc,
			attempt:         i + 1,
			maxAttempt:      maxAttempt,
			producer:        producer,
			dlqTopic:        "dead_letter_queue",
			nextRetryTopic:  fmt.Sprintf("my_topic_retry_%d", i+1),
		}
	}

	return &RetryableConsumer{
		Consumer:        consumer,
		delayCalculator: calc,
		producer:        producer,
		maxAttempt:      maxAttempt,
		dlqTopic:        "dead_letter_queue",
		retriers:        retriers,
	}
}

func TestRetryableClose(t *testing.T) {
	retryableConsumer := createTestRetryableConsumer(t, 2)

	saramaConsumer := retryableConsumer.Consumer.saramaConsumer.(*mockSaramaConsumer)
	saramaConsumer.On("Close").Once().Return(nil)
	mockProducer := retryableConsumer.producer.(*MockAsyncProducer)
	mockProducer.On("Close").Once().Return(nil)

	for i := 0; i < 2; i++ {
		saramaConsumerRetrier := retryableConsumer.retriers[i].Consumer.saramaConsumer.(*mockSaramaConsumer)
		saramaConsumerRetrier.On("Close").Once().Return(nil)
		saramaMessages := make(chan *sarama.ConsumerMessage)
		saramaConsumerRetrier.On("Messages").Once().Return(saramaMessages)

		mockProducerRetrier := retryableConsumer.retriers[i].producer.(*MockAsyncProducer)
		mockProducerRetrier.On("Close").Once().Return(nil)
	}

	retryableConsumer.Close()

	_, closed := <-retryableConsumer.doneChannel
	testingutil.Equals(t, false, closed)
	_, closed = <-retryableConsumer.retriers[0].doneChannel
	testingutil.Equals(t, false, closed)
	_, closed = <-retryableConsumer.retriers[1].doneChannel
	testingutil.Equals(t, false, closed)
}

func TestRetryableNack(t *testing.T) {
	retryableConsumer := createTestRetryableConsumer(t, 1)

	saramaMessage := &sarama.ConsumerMessage{}
	message := Message{saramaMessage}

	saramaConsumer := retryableConsumer.Consumer.saramaConsumer.(*mockSaramaConsumer)
	saramaConsumer.On("MarkOffset", saramaMessage, "").Once()
	mockProducer := retryableConsumer.producer.(*MockAsyncProducer)
	mockProducer.On("Input").Once().Return(make(chan<- *sarama.ProducerMessage, 1))
	saramaConsumerRetrier := retryableConsumer.retriers[0].Consumer.saramaConsumer.(*mockSaramaConsumer)
	saramaConsumerRetrier.On("MarkOffset", saramaMessage, "").Once()
	mockProducerRetrier := retryableConsumer.retriers[0].producer.(*MockAsyncProducer)
	mockProducerRetrier.On("Input").Once().Return(make(chan<- *sarama.ProducerMessage, 1))

	retryableConsumer.Nack(message)
	retryableConsumer.retriers[0].Nack(message)
}
