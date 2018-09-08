package kafkaclient_test

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/honestbee/kafkaclient"
	"github.com/honestbee/kafkaclient/testingutil"
)

func TestNewMessageFromSaramaMessage(t *testing.T) {
	saramaMessage := &sarama.ConsumerMessage{}
	message := kafkaclient.NewMessageFromSaramaMessage(saramaMessage)

	testingutil.Assert(t, message != nil, "Message is nil")
	testingutil.Equals(t, saramaMessage, message.ConsumerMessage)
}
