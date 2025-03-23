package queue

import (
	"log"
	"os"

	"github.com/streadway/amqp"
)

func Connect() *amqp.Connection {
	url := os.Getenv("QUEUE_URL")
	log.Println("Connecting to RabbitMQ at: ", url)
	if url == "" {
		panic("QUEUE_URL is not set")
	}

	// Use the CloudAMQP URL
	conn, err := amqp.Dial(url)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}

	return conn
}
