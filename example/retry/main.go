package main

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/honestbee/kafkaclient"
	"github.com/honestbee/kafkaclient/delaycalculator"
)

// CounterMetric is metric type for counter
type CounterMetric struct {
	Name  string
	Count int64
}

// Inc will add 1 counter to the metric
func (c *CounterMetric) Inc(delta int64, tags map[string]string) {
	c.Count += delta
	fmt.Printf("%s = %d\n", c.Name, c.Count)
}

// Monitoring for moitoring
type Monitoring struct {
	counters map[string]kafkaclient.Counter
	sync.Mutex
}

// Counter returns metric for counter
func (m *Monitoring) Counter(name string) kafkaclient.Counter {
	if counter, ok := m.counters[name]; ok {
		return counter
	}

	counter := &CounterMetric{name, 0}
	m.Lock()
	m.counters[name] = counter
	m.Unlock()

	return counter
}

var i = -1
var _ kafkaclient.Counter = new(CounterMetric)
var _ kafkaclient.Monitorer = new(Monitoring)

func main() {
	config := kafkaclient.NewDefaultConfig()
	config.ClientID = "example_retry"
	config.Group.Session.Timeout = 60 * time.Second
	config.Group.Heartbeat.Interval = 5 * time.Second
	config.Debug = true
	monitoring := new(Monitoring)
	monitoring.counters = make(map[string]kafkaclient.Counter)
	client, err := kafkaclient.NewClient([]string{"127.0.0.1:9092"}, config, kafkaclient.WithMonitorer(monitoring))
	printErr(err)
	delayCalc := delaycalculator.NewExponentialDelayCalculator(2*time.Second, 1.5)
	consumer, err := client.NewRetryableConsumer("custom", []string{"sample"}, delayCalc, 3, operation)
	printErr(err)
	defer consumer.Close()

	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	println("READY TO CONSUME")
	messages := consumer.Messages()
FORLOOP:
	for {
		select {
		case msg := <-messages:
			i++
			println("CONSUMED")
			if i%2 == 0 {
				consumer.Ack(*msg)
				println("CONSUME ACK")
				break
			}
			if succeed := operation(*msg); succeed {
				consumer.Ack(*msg)
				println("CONSUME ACK")
			} else {
				consumer.Nack(*msg)
				println("CONSUME NACK")
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

func operation(msg kafkaclient.Message) bool {
	var err error
	fmt.Printf("Message: %+v", *msg.ConsumerMessage)
	err = fmt.Errorf("Error - %d", i)

	if err != nil {
		fmt.Printf("%+v\n", err)
		return false
	}
	return true
}

func printErr(err error) {
	if err != nil {
		println("ERROR " + err.Error())
	}
}
