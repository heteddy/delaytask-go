package main

import (
	"context"
	"timewheel"
	"time"
	"timeService"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	s := timewheel.NewTaskStorageService(ctx, "redis://:uestc12345@127.0.0.1:6379/4",
		time.Minute, "messageQ", "remote-task0:")
	s.Start()
	timeService.TimerService.Start()
	time.Sleep(time.Minute * 1)
	s.Stop()
	timeService.TimerService.Stop()
	// cancel
	cancel()
}
