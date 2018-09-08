package kafkaclient_test

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/honestbee/kafkaclient"
	"github.com/honestbee/kafkaclient/testingutil"
)

func TestNewDefaultConfig(t *testing.T) {
	config := kafkaclient.NewDefaultConfig()

	testingutil.Equals(t, false, config.Group.Return.Notifications)
	testingutil.Equals(t, true, config.Consumer.Return.Errors)
	testingutil.Equals(t, sarama.OffsetNewest, config.Consumer.Offsets.Initial)
}
