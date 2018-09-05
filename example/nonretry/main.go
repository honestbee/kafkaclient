package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/honestbee/kafkaclient"
)

func main() {
	config := kafkaclient.NewDefaultConfig()
	client, err := kafkaclient.NewClient([]string{"127.0.0.1:9092"}, config, nil)
	printErr(err)
	consumer, err := client.NewConsumer("custom", []string{"sample"})
	printErr(err)
	defer consumer.Close()

	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	messages := consumer.Messages()
FORLOOP:
	for {
		select {
		case msg := <-messages:
			fmt.Printf("NONRETRY CONSUME MSG\n")
			if err := process(*msg); err != nil {
				fmt.Printf("%+v\n", err)
				consumer.Nack(*msg)
			} else {
				consumer.Ack(*msg)
			}
			if string(msg.Value) == "exit" {
				println("CONSUME EXIT")
				break FORLOOP
			}

		case <-signals:
			return
		}
	}
}

var i int

func process(msg kafkaclient.Message) error {
	defer func() {
		i++
	}()
	fmt.Printf("Message: %+v", msg)
	if i%2 == 0 {
		return nil
	}

	return fmt.Errorf("Error - %d", i)
}

func printErr(err error) {
	if err != nil {
		println(err)
	}
}
