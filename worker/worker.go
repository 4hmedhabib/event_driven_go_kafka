package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	topic := "comments"
	worker, err := connectConsumer([]string{"localhost:29092"})
	if err != nil {
		panic(err)
	}

	consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}

	fmt.Println("consumer started")
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	msgCount := 0

	doneCh := make(chan struct{})

	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println("Consumer error: ", err)
			case msg := <-consumer.Messages():
				msgCount++
				fmt.Printf("Received message count: %d: | Topic (%s) | Message(%s)\n", msgCount, string(msg.Topic), string(msg.Value))
			case <-sigChan:
				fmt.Println("Interruption detected")
				doneCh <- struct{}{}
			}

		}
	}()

	<-doneCh
	fmt.Println("Processed", msgCount, "messages")
	if err := worker.Close(); err != nil {
		panic(err)
	}
}

func connectConsumer(brokerURL []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	conn, err := sarama.NewConsumer(brokerURL, config)
	if err != nil {
		log.Println("Connection consumer error: ", err)
		return nil, err
	}

	return conn, nil
}
