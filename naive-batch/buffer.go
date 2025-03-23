package main

import (
	"fmt"
	"log"
	"sync"

	"github.com/harshjoeyit/hashtag-extraction/db"
)

var buffer *HashTagBuffer
var flushThreshold = 500 // flush the buffer every 1000 posts

type HashTagBuffer struct {
	M  map[string]int
	Mu sync.Mutex
}

func NewHashTagBuffer() *HashTagBuffer {
	return &HashTagBuffer{
		M:  make(map[string]int),
		Mu: sync.Mutex{},
	}
}

func (b *HashTagBuffer) Get(key string) int {
	b.Mu.Lock()
	defer b.Mu.Unlock()

	if val, ok := b.M[key]; ok {
		return val
	}

	return 0
}

// Inc increments the count of the key in the buffer
func (b *HashTagBuffer) Inc(key string, val int) {
	b.Mu.Lock()
	defer b.Mu.Unlock()

	b.M[key] += val
}

// Flush updates the counts in the database for all hashtags
// in the buffer and resets the buffer
func (b *HashTagBuffer) Flush() {
	b.Mu.Lock()
	defer b.Mu.Unlock()

	fmt.Printf("flushing %d hash tags\n", len(b.M))

	// Update the counts in DB for all hashtags
	for k, v := range b.M {
		if v == 0 {
			continue
		}

		err := db.Inc(k, v)
		if err != nil {
			log.Printf("Failed to update db for hashtag: %s, error: %v", k, err)
		}
	}

	// Reset the buffer
	b.M = make(map[string]int)
}
