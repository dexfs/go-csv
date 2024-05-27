package main

import (
	"context"
	"log/slog"
)

func main() {
	myChannel := make(chan string)
	anotherChannel := make(chan string)

	go func() {
		myChannel <- "data"
	}()

	go func() {
		anotherChannel <- "cow"
	}()

	select {
	case msgFromMyChannel := <-myChannel:
		log(msgFromMyChannel)
	case msgFromAnotherChannel := <-anotherChannel:
		log(msgFromAnotherChannel)
	}
}

func log(msg string) {
	ctx := context.Background()
	slog.Log(ctx, slog.LevelInfo, msg)

}
