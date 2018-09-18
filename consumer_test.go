package kafkaclient

import (
	"sync"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/honestbee/kafkaclient/testingutil"
	"github.com/stretchr/testify/mock"
)

func createTestConsumer(t *testing.T, monitorer Monitorer) *Consumer {
	consumer := &Consumer{
		saramaConsumer: &mockSaramaConsumer{},
		doneChannel:    make(chan struct{}),
		monitorer:      monitorer,
		logger:         new(MockLogger),
		consumerGroup:  "my_group",
		topics:         []string{"my_topic"},
	}

	return consumer
}

func TestMessages(t *testing.T) {
	startedCounter := &MockCounter{}
	startedCounter.On("Inc", int64(1), mock.AnythingOfType("map[string]string")).Once()

	messageInCounter := &MockCounter{}
	messageInCounter.On("Inc", int64(1), mock.AnythingOfType("map[string]string"))

	monitorer := &MockMonitorer{}
	monitorer.On("Counter", mock.AnythingOfType("string")).Return(func(s string) Counter {
		if s == KafkaConsumerStarted {
			return startedCounter
		}

		return messageInCounter
	})

	consumer := createTestConsumer(t, monitorer)
	saramaMessages := make(chan *sarama.ConsumerMessage)
	saramaConsumer := consumer.saramaConsumer.(*mockSaramaConsumer)
	saramaConsumer.On("Messages").Once().Return(saramaMessages)

	consumer.saramaConsumer = saramaConsumer
	consumerMessages := consumer.Messages()

	wg := sync.WaitGroup{}
	wg.Add(2)
	saramaMessage1 := &sarama.ConsumerMessage{}
	saramaMessage1.Offset = 1
	saramaMessage2 := &sarama.ConsumerMessage{}
	saramaMessage2.Offset = 2
	go func() {
		saramaMessages <- saramaMessage1
		saramaMessages <- saramaMessage2
		wg.Done()
	}()
	go func() {
		msg1 := <-consumerMessages
		msg2 := <-consumerMessages
		testingutil.Equals(t, int64(1), msg1.Offset)
		testingutil.Equals(t, int64(2), msg2.Offset)
		wg.Done()
	}()
	wg.Wait()
}

func TestAck(t *testing.T) {
	counter := &MockCounter{}
	counter.On("Inc", int64(1), mock.AnythingOfType("map[string]string")).Once()

	monitorer := &MockMonitorer{}
	monitorer.On("Counter", KafkaPartitionMessagesAck).Once().Return(counter)

	consumer := createTestConsumer(t, monitorer)
	saramaMessage := &sarama.ConsumerMessage{}
	message := Message{saramaMessage}
	saramaConsumer := &mockSaramaConsumer{}
	saramaConsumer.On("MarkOffset", saramaMessage, "").Once()

	consumer.saramaConsumer = saramaConsumer
	consumer.Ack(message)
}

func TestNack(t *testing.T) {
	ackCounter := &MockCounter{}
	ackCounter.On("Inc", int64(1), mock.AnythingOfType("map[string]string")).Once()

	nackCounter := &MockCounter{}
	nackCounter.On("Inc", int64(1), mock.AnythingOfType("map[string]string")).Once()

	monitorer := &MockMonitorer{}
	monitorer.On("Counter", mock.AnythingOfType("string")).Return(func(s string) Counter {
		if s == KafkaPartitionMessagesAck {
			return ackCounter
		}

		return nackCounter
	})

	consumer := createTestConsumer(t, monitorer)
	saramaMessage := &sarama.ConsumerMessage{}
	message := Message{saramaMessage}
	saramaConsumer := consumer.saramaConsumer.(*mockSaramaConsumer)
	saramaConsumer.On("MarkOffset", saramaMessage, "").Once()

	consumer.saramaConsumer = saramaConsumer
	consumer.Nack(message)
}

func TestClose(t *testing.T) {
	counter := &MockCounter{}
	counter.On("Inc", int64(1), mock.AnythingOfType("map[string]string")).Once()

	monitorer := &MockMonitorer{}
	monitorer.On("Counter", KafkaConsumerClosed).Once().Return(counter)

	consumer := createTestConsumer(t, monitorer)
	saramaConsumer := consumer.saramaConsumer.(*mockSaramaConsumer)
	saramaConsumer.On("Close").Once().Return(nil)

	consumer.saramaConsumer = saramaConsumer
	consumer.Close()

	_, closed := <-consumer.doneChannel
	testingutil.Equals(t, false, closed)
}
