package timewheel

import (
	"testing"
	"context"
	"time"
	"fmt"
	"timeService"
)

func TestSubscriber(t *testing.T) {
	//ctx context.Context, url string, keepalive time.Duration, topic string,
	//	namePrefix string, taskComing TaskComingFunc
	ctx, cancel := context.WithCancel(context.Background())

	s := NewTaskStorageService(ctx, "redis://:uestc12345@127.0.0.1:6379/4",
		time.Minute, "remote-task0:distribute", "remote-task0", func(channel string, message []byte) bool {
			fmt.Println(channel)
			fmt.Println(string(message))
			return true
		})
	s.Start()
	time.Sleep(time.Second * 20)
	s.Stop()
	// cancel
	cancel()
	t.Log("worker ok")

}
func TestAppendToTaskTable(t *testing.T) {
	ctx, _ := context.WithCancel(context.Background())

	s := NewTaskStorageService(ctx, "redis://:uestc12345@127.0.0.1:6379/4",
		time.Minute, "remote-task0:distribute", "remote-task0", func(channel string, message []byte) bool {
			fmt.Println(channel)
			fmt.Println(string(message))
			return true
		})
	s.Start()
	timeService.TimerService.Start()
	fmt.Println("starting appendtasktable")
	s.AppendToTaskTable(`{"id":"135819246216290305","name":"PeriodSleepTask","torunat":"2019-01-10T18:57:13.697764+08:00",
	"torunafter":2000000000,"cost":0,"done":0,"timeout":2000000000,"runat":"0001-01-01T00:00:00Z","Interval":2000000000,
	"EndTime":"2019-01-10T19:00:11.697764+08:00"}
	`)
	s.AppendToTaskTable(`{"id":"135819246216290306","name":"PeriodSleepTask","torunat":"2019-01-10T18:57:23.697955+08:00",
	"torunafter":12000000000,"cost":0,"done":0,"timeout":2000000000,"runat":"0001-01-01T00:00:00Z","Interval":12000000000,
	"EndTime":"2019-01-10T19:00:11.697955+08:00"}
	`)
	s.AppendToTaskTable(`{"id":"135819246216290307","name":"PeriodSleepTask","torunat":"2019-01-10T18:57:23.697979+08:00",
	"torunafter":12000000000,"cost":0,"done":0,"timeout":2000000000,"runat":"0001-01-01T00:00:00Z","Interval":12000000000,
	"EndTime":"2019-01-10T19:00:11.697979+08:00"}
	`)
	s.AppendToTaskTable(`{"id":"135819246216290308","name":"PeriodSleepTask","torunat":"2019-01-10T18:57:23.697987+08:00",
	"torunafter":12000000000,"cost":0,"done":0,"timeout":2000000000,"runat":"0001-01-01T00:00:00Z","Interval":12000000000,
	"EndTime":"2019-01-10T19:00:11.697987+08:00"}
	`)
	s.GetTaskInfo("135819246216290305")
	time.Sleep(time.Second * 20)
	s.Stop()
	timeService.TimerService.Stop()
	// cancel
	//cancel()
	t.Log("worker ok")
}
func TestInsertToWaitingQ(t *testing.T) {
	t.Log("worker ok")
}
func TestMoveWaitingToOngoingQ(t *testing.T) {
	t.Log("worker ok")
}
func TestChangeTaskToComplete(t *testing.T) {
	t.Log("worker ok")
}
func TestRemoveFromTaskTable(t *testing.T) {
	t.Log("worker ok")
}
func TestGetAllTaskTable(t *testing.T) {
	ctx, _ := context.WithCancel(context.Background())

	s := NewTaskStorageService(ctx, "redis://:uestc12345@127.0.0.1:6379/4",
		time.Minute, "remote-task0:distribute", "remote-task0", func(channel string, message []byte) bool {
			fmt.Println(channel)
			fmt.Println(string(message))
			return true
		})
	s.Start()
	timeService.TimerService.Start()
	s.GetTaskInfo("135819246216290305")
	time.Sleep(time.Second * 20)
	s.Stop()
	timeService.TimerService.Stop()
	// cancel
	//cancel()
	t.Log("worker ok")
}
