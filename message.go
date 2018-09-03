package kafkaclient

import "github.com/Shopify/sarama"

// Message from kafka
type Message struct {
	*sarama.ConsumerMessage
}

// NewMessageFromSaramaMessage returns a new Message based on sarama message
func NewMessageFromSaramaMessage(msg *sarama.ConsumerMessage) *Message {
	return &Message{msg}
}

func newSaramaProducerMessage(topic string, key, value []byte) *sarama.ProducerMessage {
	return &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
	}
}
