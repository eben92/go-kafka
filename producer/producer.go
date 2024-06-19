package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/IBM/sarama"
	"github.com/gofiber/fiber/v2"
)

type Comment struct {
	Text string `form:"text" json:"text"`
}

func main() {
	app := fiber.New()
	api := app.Group("/api/v1")
	api.Post("/comments", createComment)
	_ = app.Listen(":3000")
}

func createComment(c *fiber.Ctx) error {
	cmt := &Comment{}

	if err := c.BodyParser(cmt); err != nil {
		log.Println(err)
		_ = c.Status(http.StatusBadRequest).JSON(&fiber.Map{
			"success": "false",
			"nessage": err,
		})

		return err

	}

	cmtInByte, _ := json.Marshal(cmt)
	if err := PushToCommentQueue("comments", cmtInByte); err != nil {
		_ = c.Status(http.StatusBadRequest).JSON(&fiber.Map{
			"success": "false",
			"nessage": err,
		})

		return err
	}

	err := c.JSON(&fiber.Map{
		"success": true,
		"message": "Comment pushed successfully",
		"comment": cmt,
	})
	if err != nil {

		log.Println(err)
		_ = c.Status(http.StatusBadRequest).JSON(&fiber.Map{
			"success": "false",
			"nessage": err,
		})

	}
	return nil
}

func connectProducer(brokersUrl []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	conn, err := sarama.NewSyncProducer(brokersUrl, config)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func PushToCommentQueue(topic string, message []byte) error {
	brokersUrl := []string{":9091", ":9092", ":9093"}

	producer, err := connectProducer(brokersUrl)
	if err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}

	log.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)", topic, partition, offset)
	return nil
}
