package main

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/gofiber/fiber/v2"
	"log"
)

type Comment struct {
	Text string `form:"text" json:"text"`
}

func main() {
	app := fiber.New()
	api := app.Group("/api/v1")
	api.Post("/comments", createComment)
	err := app.Listen("127.0.0.1:8080")
	if err != nil {
		log.Println("Starting server error: ", err)
		return
	}
}

func ConnectProducer(brokerURL []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	conn, err := sarama.NewSyncProducer(brokerURL, config)

	if err != nil {
		log.Println("Connection producer error: ", err)
		return nil, err
	}

	return conn, nil
}

func PushCommentToQueue(topic string, message []byte) error {
	brokerURL := []string{"localhost:29092"}
	producer, err := ConnectProducer(brokerURL)

	if err != nil {
		return err
	}
	defer func(producer sarama.SyncProducer) {
		err := producer.Close()
		if err != nil {
			log.Println("Error to close producer: ", err)
		}
	}(producer)

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)

	fmt.Printf("Message is stored in top(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)

	return nil
}
func createComment(c *fiber.Ctx) error {
	cmt := new(Comment)
	if err := c.BodyParser(cmt); err != nil {
		log.Println("Request body parser error: ", err)
		_ = c.Status(400).JSON(&fiber.Map{
			"success": false,
			"message": err,
		})
		return err
	}

	cmtInBytes, err := json.Marshal(cmt)
	_ = PushCommentToQueue("comments", cmtInBytes)

	_ = c.JSON(&fiber.Map{
		"success": true,
		"message": "Comment pushed successfully",
		"comment": cmt,
	})

	if err != nil {
		log.Println("Json object marshal error: ", err)
		_ = c.Status(400).JSON(&fiber.Map{
			"success": false,
			"message": err,
		})
		return err
	}

	return err
}
