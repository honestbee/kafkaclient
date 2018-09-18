package kafkaclient

import (
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/honestbee/kafkaclient/delaycalculator"
	"github.com/honestbee/kafkaclient/testingutil"
)

func TestNewClient(t *testing.T) {
	testCases := []struct {
		brokers           []string
		config            *Config
		monitorer         Monitorer
		expectedErrString string
	}{
		{
			brokers:           []string{},
			config:            NewDefaultConfig(),
			monitorer:         nil,
			expectedErrString: "kafka: invalid configuration (You must provide at least one broker address)",
		},
		{
			brokers:           []string{"127.0.0.1:9092"},
			config:            nil,
			monitorer:         nil,
			expectedErrString: ErrConfigIsRequired.Error(),
		},
	}

	for _, tc := range testCases {
		client, err := NewClient(tc.brokers, tc.config, WithMonitorer(tc.monitorer))

		if tc.expectedErrString != "" {
			testingutil.Equals(t, tc.expectedErrString, err.Error())
			testingutil.Assert(t, client == nil, "Client is nil")
		} else {
			testingutil.Ok(t, err)
			testingutil.Assert(t, client != nil, "Client is not nil")
		}
	}
}

func TestNewRetryableConsumer(t *testing.T) {
	config := NewDefaultConfig()
	client := &Client{}
	client.config = config
	client.logger = new(MockLogger)
	operate := func(msg Message) bool {
		return true
	}

	calc := delaycalculator.NewLinearDelayCalculator(0 * time.Second)
	consumer, err := client.newRetryableConsumer("my_group", []string{"my_topic"}, calc, 2, operate, newTestConsumer, newTestSyncProducer)
	testingutil.Ok(t, err)

	for i := 0; i < len(consumer.retriers); i++ {
		producer := consumer.retriers[i].producer.(*MockSyncProducer)
		producer.On("Close").Once().Return(nil)

		saramaConsumer := consumer.retriers[i].Consumer.saramaConsumer.(*mockSaramaConsumer)
		saramaConsumer.On("Close").Once().Return(nil)
		saramaMessages := make(chan *sarama.ConsumerMessage)
		saramaConsumer.On("Messages").Once().Return(saramaMessages)
		saramaConsumer.On("Errors").Once().Return(make(chan error))
	}

	producer := consumer.producer.(*MockSyncProducer)
	producer.On("Close").Once().Return(nil)

	saramaConsumer := consumer.Consumer.saramaConsumer.(*mockSaramaConsumer)
	saramaConsumer.On("Close").Once().Return(nil)

	testingutil.Equals(t, "my_group", consumer.consumerGroup)
	testingutil.Equals(t, "my_topic", consumer.topics[0])
	testingutil.Equals(t, "my_topic_retry_1", consumer.nextRetryTopic)

	testingutil.Equals(t, 2, len(consumer.retriers))
	testingutil.Equals(t, "my_group_retry_1", consumer.retriers[0].consumerGroup)
	testingutil.Equals(t, "my_topic_retry_1", consumer.retriers[0].topics[0])
	testingutil.Equals(t, 2, consumer.retriers[0].maxAttempt)
	testingutil.Equals(t, 1, consumer.retriers[0].attempt)
	testingutil.Equals(t, "my_topic_retry_2", consumer.retriers[0].nextRetryTopic)
	testingutil.Equals(t, "dead_letter_queue", consumer.retriers[0].dlqTopic)
	testingutil.Equals(t, 2, consumer.retriers[1].attempt)
	testingutil.Equals(t, "dead_letter_queue", consumer.retriers[1].nextRetryTopic)
	testingutil.Equals(t, "dead_letter_queue", consumer.retriers[1].dlqTopic)

	consumer.Close()
}
