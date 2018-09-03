package kafkaclient

import (
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

const (
	defaultDLQTopic = "dead_letter_queue"
)

// Config config
type Config struct {
	cluster.Config
	DLQTopic string
}

// NewDefaultConfig returns a new configuration instance with sane defaults.
func NewDefaultConfig() *Config {
	c := cluster.NewConfig()
	config := &Config{*c, defaultDLQTopic}
	config.Group.Return.Notifications = false
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	return config
}
