package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/harshjoeyit/hashtag-extraction/db"
	post "github.com/harshjoeyit/hashtag-extraction/posts"
	"github.com/harshjoeyit/hashtag-extraction/queue"
	"github.com/joho/godotenv"
	"github.com/streadway/amqp"
)

const W = 5      // Number of update workers
const N = 10_000 // Number of posts to publish

var consumedPosts int // number of posts consumed
var mu sync.Mutex     // mutex for consumedPosts
var start time.Time   // start time of post messages consumption

// UpdateWorker consumes post messages from the queue and
// updates the hashtag counts in the database
func UpdateWorker(postMsgs <-chan amqp.Delivery, buffer *HashTagBuffer, workerID int) {
	for m := range postMsgs {
		// log.Printf("workerID: %d Received a message: %s", workerID, m.Body)

		post := &post.Post{}

		// Parse the message
		err := json.Unmarshal(m.Body, &post)
		if err != nil {
			log.Printf("Failed to parse the message: %s", err)
			continue
		}

		// Update the hashtag count in buffer
		for _, h := range post.ExtractHashtags() {
			buffer.Inc(h, 1)
		}

		mu.Lock()
		consumedPosts++

		if consumedPosts%flushThreshold == 0 {
			// Flush the buffer in a goroutine
			go func() {
				buffer.Flush()
			}()
		}

		if consumedPosts%(N/10) == 0 {
			log.Printf("Consumed %d posts in %v", consumedPosts, time.Since(start))
		}

		if consumedPosts == N {
			log.Println("All posts consumed in ", time.Since(start))
		}

		mu.Unlock()
	}
}

func UpdateHashTagCounts() {
	// Consume post events from the queue
	conn := queue.Connect()
	defer conn.Close()

	// Open a channel
	amqpChan, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %s", err)
	}

	// Declare a queue
	q, err := amqpChan.QueueDeclare(
		"posts", // queue name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %s", err)
	}

	// Consume messages
	postMsgs, err := amqpChan.Consume(
		q.Name,           // queue
		"naive-consumer", // consumer
		true,             // auto-ack
		false,            // exclusive
		false,            // no-local
		false,            // no-wait
		nil,              // args
	)
	if err != nil {
		log.Fatalf("Failed to register a consumer: %s", err)
	}

	forever := make(chan struct{})

	buffer = NewHashTagBuffer()

	// Start update workers
	for w := range W {
		go func(id int) {
			UpdateWorker(postMsgs, buffer, id)
		}(w)
	}

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")

	<-forever
}

func main() {
	// Load .env file
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	db.Init()

	post.Publish(N) // Publish N posts

	start = time.Now()
	UpdateHashTagCounts() // Update hashtag counts
}
