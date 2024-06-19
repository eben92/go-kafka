package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {
	topic := "comments"

	consumer, err := connectConsumer([]string{":9091", ":9092", ":9093"})
	if err != nil {
		panic(err)
	}

	c, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}

	log.Println("consumer started")
	ch := make(chan os.Signal, 1)

	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)

	msgCount := 0

	dch := make(chan struct{})

	go func() {
		for {
			select {
			case err := <-c.Errors():
				log.Println(err)
			case msg := <-c.Messages():
				msgCount++
				log.Printf(
					"Recieved message count: %d: | topic (%s) | message (%s)",
					msgCount,
					string(msg.Topic),
					string(msg.Value),
				)
			case <-ch:
				log.Println("Interruption detected")
				dch <- struct{}{}
			}
		}
	}()

	<-dch
	log.Println("Processed", msgCount, "messages")
	if err := consumer.Close(); err != nil {
		panic(err)
	}
}

func connectConsumer(brokerUrl []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	conn, err := sarama.NewConsumer(brokerUrl, config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}
