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
	Debug    bool
}

// NewDefaultConfig returns a new configuration instance with sane defaults.
func NewDefaultConfig() *Config {
	c := cluster.NewConfig()
	config := &Config{*c, defaultDLQTopic, false}
	config.Group.Return.Notifications = false
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	return config
}
