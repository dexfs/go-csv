package main

import (
	"context"
	"log/slog"
	"time"
)

func doWork(done <-chan bool) {
	for {
		select {
		case <-done:
			return
		default:
			log("DOING WORK!")
		}
	}
}

func main() {
	done := make(chan bool)
	go doWork(done)
	time.Sleep(time.Second * 3)
	close(done)
}

func log(msg string) {
	ctx := context.Background()
	slog.Log(ctx, slog.LevelInfo, msg)

}
