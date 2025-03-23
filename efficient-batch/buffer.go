package main

import (
	"log"
	"sync"

	"github.com/harshjoeyit/hashtag-extraction/db"
)

var buffer *HashTagBuffer
var flushThreshold = 500 // flush the buffer every 1000 posts

type HashTagBuffer struct {
	// ActiveM is used to increment counts on consuming post messages from the queue
	ActiveM map[string]int

	// PassiveM is used to update counts in the database
	PassiveM map[string]int

	// Channel to signal that passive buffer is flushed to the database
	Flushed chan struct{}

	Mu sync.Mutex
}

func NewHashTagBuffer() *HashTagBuffer {
	b := &HashTagBuffer{
		ActiveM:  make(map[string]int),
		PassiveM: make(map[string]int),
		Flushed:  make(chan struct{}, 1), // buffered channel
		Mu:       sync.Mutex{},
	}

	// For the first call to Flush to not block signal that the
	// passive buffer is flushed
	b.Flushed <- struct{}{}

	return b
}

// Inc increments the count of the key in the active buffer
func (b *HashTagBuffer) Inc(key string, val int) {
	b.Mu.Lock()
	defer b.Mu.Unlock()

	b.ActiveM[key] += val
}

// Flush updates the counts in the database for all hashtags
// in the buffer and resets the buffer
func (b *HashTagBuffer) Flush() {
	// Lock before flushing
	b.Mu.Lock()

	// log.Println("Before blocking, active buffer size: ", len(b.ActiveM), "passive buffer size: ", len(b.PassiveM))

	// Block until passive buffer is flushed
	<-b.Flushed

	// log.Println("After blocking, active buffer size: ", len(b.ActiveM), "passive buffer size: ", len(b.PassiveM))

	// Swap the active and passive buffers
	b.ActiveM, b.PassiveM = b.PassiveM, b.ActiveM

	// Unlock before updating the DB
	b.Mu.Unlock()

	log.Printf("flushing %d hash tags\n", len(b.PassiveM))

	// Update the counts in DB for all hashtags using the passive
	for k, v := range b.PassiveM {
		if v == 0 {
			continue
		}

		err := db.Inc(k, v)
		if err != nil {
			log.Printf("Failed to update db for hashtag: %s, error: %v", k, err)
		}
	}

	// Reset the passive buffer
	b.PassiveM = make(map[string]int)

	// Signal that the passive buffer is flushed
	b.Flushed <- struct{}{}
}
