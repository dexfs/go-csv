package main

import (
	"context"
	"log/slog"
)

// unbuffered channel is a sychronous communication -> make(chan string) -> no pass size
// buffered channel is async communication -> make(chan string, 3)
func main() {
	charChannel := make(chan string, 3)
	chars := []string{"a", "b", "c"}

	for _, char := range chars {
		select {
		case charChannel <- char:
		}
	}

	close(charChannel)

	for result := range charChannel {
		log(result)
	}

}

func log(msg string) {
	ctx := context.Background()
	slog.Log(ctx, slog.LevelInfo, msg)

}
