package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"time"

	"github.com/Shopify/sarama"
	"github.com/honestbee/kafkaclient"
	"github.com/sirupsen/logrus"
)

var retryTopicRegexp = regexp.MustCompile("(.*_retry_)\\d+")

func main() {
	kafkaHost := flag.String("host", "127.0.0.1:9092", "kafka host")
	dlqTopic := flag.String("dlqTopic", "dead_letter_queue", "dlq topic name")
	startingOffset := flag.Int64("startingOffset", sarama.OffsetOldest, "starting offset")
	flag.Parse()

	config := kafkaclient.NewDefaultConfig()
	if *startingOffset == -1 {
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	} else {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}
	client, err := kafkaclient.NewClient([]string{*kafkaHost}, config)
	handleFatalError(err)

	producer, err := client.NewSyncProducer()
	handleFatalError(err)
	defer producer.Close()
	consumer, err := client.NewConsumer(fmt.Sprintf("dlqretrier_%d", time.Now().Unix()), []string{*dlqTopic})
	handleFatalError(err)
	defer consumer.Close()

	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	messages := consumer.Messages()
CONSUMELOOP:
	for {
		select {
		case msg := <-messages:
			// skip all messages before the specified offset
			if msg.Offset < *startingOffset {
				consumer.Ack(*msg)
				break
			}

			logrus.WithFields(logrus.Fields{
				"Partition": msg.Partition,
				"Offset":    msg.Offset,
				"Value":     string(msg.Value),
			}).Println("processing message")
			var dlqMessageValue kafkaclient.DLQMessageValue
			if err := json.Unmarshal(msg.Value, &dlqMessageValue); err != nil {
				logrus.WithError(err).Errorln("fail to unmarshal the message")
				consumer.Nack(*msg)
				break
			}
			if dlqMessageValue.LastTopic == "" || len(dlqMessageValue.OriginalValue) == 0 {
				logrus.WithFields(logrus.Fields{
					"DLQMessageValue": dlqMessageValue,
				}).Println("unsupported dlq message")
				consumer.Nack(*msg)
				break
			}

			retryTopic := getRetryTopic(dlqMessageValue.LastTopic)
			newMessage := kafkaclient.NewSaramaProducerMessage(retryTopic, msg.Key, dlqMessageValue.OriginalValue)
			_, _, err := producer.SendMessage(newMessage)
			if err != nil {
				logrus.WithError(err).WithFields(logrus.Fields{
					"DestinationTopic": retryTopic,
				}).Errorln("fail to publish to retry topic")
				consumer.Nack(*msg)
				break
			}

			consumer.Ack(*msg)
		case <-signals:
			break CONSUMELOOP
		}
	}
}

func handleFatalError(err error) {
	if err != nil {
		logrus.Fatalln(err)
	}
}

func getRetryTopic(lastTopic string) string {
	if !retryTopicRegexp.MatchString(lastTopic) {
		return lastTopic
	}

	return retryTopicRegexp.ReplaceAllString(lastTopic, "${1}1") // get the first retry topic
}
