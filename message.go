package kafkaclient

import (
	"encoding/json"

	"github.com/Shopify/sarama"
)

// Message from kafka
type Message struct {
	*sarama.ConsumerMessage
}

// DLQMessageValue is a value for DLQ Message
type DLQMessageValue struct {
	OriginalValue []byte `json:"original_value"`
	LastTopic     string `json:"last_topic"`
}

// NewMessageFromSaramaMessage returns a new Message based on sarama message
func NewMessageFromSaramaMessage(msg *sarama.ConsumerMessage) *Message {
	return &Message{msg}
}

func NewSaramaProducerMessage(topic string, key, value []byte) *sarama.ProducerMessage {
	return &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
	}
}

func NewSaramaProducerDLQMessage(msg Message, dlqTopic string) (*sarama.ProducerMessage, error) {
	dlqMessageValue := DLQMessageValue{
		OriginalValue: msg.Value,
		LastTopic:     msg.Topic,
	}
	dlqMessageByte, err := json.Marshal(dlqMessageValue)
	if err != nil {
		return nil, err
	}

	return &sarama.ProducerMessage{
		Topic: dlqTopic,
		Key:   sarama.ByteEncoder(msg.Key),
		Value: sarama.ByteEncoder(dlqMessageByte),
	}, nil
}
