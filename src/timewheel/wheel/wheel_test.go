package wheel

import (
	"github.com/sirupsen/logrus"
	"testing"
	"time"
	"wheelLogger"
	myTrace "trace"
	"encoding/json"
	"fmt"
)

type PeriodSleepTask struct {
	PeriodicTask
}

func (pTask *PeriodSleepTask) Run() (bool, error) {
	wheelLogger.Logger.WithFields(logrus.Fields{
		"now":    time.Now().Format("2006-01-02 15:04:05.999999"),
		"taskID": pTask.ID,
	}).Infoln("starting running task")
	pTask.RunAt = time.Now()
	time.Sleep(time.Second * 1)
	pTask.Cost = time.Now().Sub(pTask.RunAt)
	wheelLogger.Logger.WithFields(logrus.Fields{
		"now":        time.Now().Format("2006-01-02 15:04:05.999999"),
		"task.RunAt": time.Now().Format("2006-01-02 15:04:05.999999"),
		"taskID":     pTask.ID,
	}).Infoln("complete running task")

	return true, nil
}

func NewPeriodSleepTask(requestID int64, interval time.Duration, end time.Time) *PeriodSleepTask {
	t := &PeriodSleepTask{}
	t.Timeout = time.Second * 2
	t.ID = requestID
	t.Name = "PeriodSleepTask"
	t.ToRunAt = time.Now().Add(interval)
	t.Interval = interval
	t.ToRunAfter = interval
	t.EndTime = end
	return t
}
func (pTask *PeriodSleepTask) Next(wheel Wheeler) bool {
	if time.Now().UnixNano() < pTask.EndTime.UnixNano() {
		// 修改下次运行的时间
		wheelLogger.Logger.WithFields(logrus.Fields{
			"now":    time.Now().Format("2006-01-02 15:04:05.999999"),
			"taskID": pTask.ID,
		}).Infoln("Next(),will add task again")
		pTask.ToRunAfter = pTask.Interval
		wheel.Add(pTask)
	} else {
	}
	return true
}

func TestWheel(t *testing.T) {

	trace := myTrace.NewTrace(0x222)

	periodTasks := []string{
		"2s", "12s", "12s", "12s", "22s", "32s", "42s", "3s", "4s", "5s", "6s", "7s", // "1024s",
	}
	tasks := make([]Runner, len(periodTasks))
	endDuration := time.Minute * 3

	w := NewTimeWheel("1s", 10, 5)
	w.Start()

	for idx, p := range periodTasks {
		interval, _ := time.ParseDuration(p)
		tasks[idx] = NewPeriodSleepTask(int64(trace.GetID()), interval, time.Now().Add(endDuration))
		jsonTask , err := json.Marshal(tasks[idx])
		fmt.Println(string(jsonTask))
		fmt.Println(err)
		//w.Add(t)
	}
	for _, t := range tasks {
		w.Add(t)
	}

	select {
	case <-time.After(time.Second * 35):
	}
	w.Stop()

	t.Log("ok")
}
