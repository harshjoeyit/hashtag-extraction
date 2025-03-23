package post

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/google/uuid"
)

const maxHashtags = 5
const hashtagLen = 2

func NewPost() *Post {
	return &Post{
		ID:      uuid.New().String(),
		Caption: generateCaption(),
	}
}

func (p *Post) ExtractHashtags() []string {
	// Extract hashtags from caption
	ww := strings.Split(p.Caption, " ")
	var hh []string

	for _, w := range ww {
		if strings.HasPrefix(w, "#") {
			hh = append(hh, w[1:]) // remove '#' from the hashtag
		}
	}

	return hh
}

// generateCaption generates a random caption with hashtags
func generateCaption() string {
	N := rand.Intn(maxHashtags) + 1
	hh := make([]string, N)

	for range N {
		hh = append(hh, getRandomHashTag())
	}

	return fmt.Sprintf("Post caption %s", strings.Join(hh, " "))
}

// getRandomHashTag generates a random hashtag of length 5
func getRandomHashTag() string {
	letters := []rune("abcdefghijklmnopqrstuvwxyz")
	h := make([]rune, hashtagLen)

	for i := range h {
		h[i] = letters[rand.Intn(len(letters))]
	}

	return "#" + string(h)
}
