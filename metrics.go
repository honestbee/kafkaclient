package kafkaclient

// Counters
const (
	KafkaConsumerStarted = "kafka.consumer.started"
	KafkaConsumerClosed  = "kafka.consumer.closed"

	KafkaPartitionMessagesIn   = "kafka.partition.messages-in"
	KafkaPartitionMessagesAck  = "kafka.partition.messages-ack"
	KafkaPartitionMessagesNack = "kafka.partition.messages-nack"

	KafkaPublishToRetryTopic = "kafka.publish.retry-topic"
	KafkaPublishToDLQ        = "kafka.publish.dlq"
)

// incCounter is a helper method to increase counter by 1
func incCounter(monitorer Monitorer, name string, tags map[string]string) {
	if monitorer != nil {
		monitorer.Counter(name).Inc(1, tags)
	}
}
