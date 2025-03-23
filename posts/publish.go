package post

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/harshjoeyit/hashtag-extraction/queue"
	"github.com/streadway/amqp"
)

const defaultNumPublishWorkers = 10

// Publish posts numPostsToPublish posts to the queue
func Publish(N int) {

	// Connect to RabbitMQ
	conn := queue.Connect()
	defer conn.Close()

	// Open a channel
	amqpChan, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %s", err)
	}
	defer amqpChan.Close()

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

	postsCh := make(chan *Post, N)
	var wg sync.WaitGroup

	// Start publish workers
	for w := 1; w <= defaultNumPublishWorkers; w++ {
		wg.Add(1)
		go publishWorker(amqpChan, q, postsCh, &wg, fmt.Sprintf("W%d", w))
	}

	// Publish posts
	for range N {
		// Send the post to the channel
		postsCh <- NewPost()
	}

	// Close the channel
	close(postsCh)

	// Wait for all workers to finish
	wg.Wait()

	log.Println("All posts have been published")
}

// publishWorker reads posts from a channel and publishes them
func publishWorker(amqpCh *amqp.Channel, q amqp.Queue, postCh <-chan *Post, wg *sync.WaitGroup, workerName string) {
	defer wg.Done()

	// Read posts from the channel
	for post := range postCh {
		err := publishPost(amqpCh, q, post)
		if err != nil {
			log.Fatalf("Failed to post message: %s", err)
		}

		// log.Printf("%s: Published a post with ID %s, Caption: %s\n", workerName, post.ID, post.Caption)
	}
}

// publishPost publishes a post to the queue
func publishPost(amqpCh *amqp.Channel, q amqp.Queue, post *Post) error {
	body, err := json.Marshal(post)
	if err != nil {
		return fmt.Errorf("failed to marshal a post: %s", err)
	}

	err = amqpCh.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		})
	if err != nil {
		return fmt.Errorf("failed to publish a message: %s", err)
	}

	return nil
}
