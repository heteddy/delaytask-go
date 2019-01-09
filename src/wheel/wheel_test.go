package wheel_test

import (
	"github.com/sirupsen/logrus"
	"testing"
	"time"
	"timewheel"
	"timewheel/logger"
	"timewheel/wheel"
)

type PeriodSleepTask struct {
	wheel.PeriodicTask
}

func (pTask *PeriodSleepTask) Run() (bool, error) {
	logger.Logger.WithFields(logrus.Fields{
		"now":    time.Now().Format("2006-01-02 15:04:05.999999"),
		"taskID": pTask.ID,
	}).Infoln("starting running task")
	pTask.RunAt = time.Now()
	time.Sleep(time.Second * 1)
	pTask.Cost = time.Now().Sub(pTask.RunAt)
	logger.Logger.WithFields(logrus.Fields{
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
	t.TaskType = wheel.PeriodTask
	t.Interval = interval
	t.ToRunAfter = interval
	t.EndTime = end

	return t
}
func (pTask *PeriodSleepTask) Next(wheel wheel.Wheeler) bool {
	if time.Now().UnixNano() < pTask.EndTime.UnixNano() {
		// 修改下次运行的时间
		logger.Logger.WithFields(logrus.Fields{
			"now":    time.Now().Format("2006-01-02 15:04:05.999999"),
			"taskID": pTask.ID,
		}).Infoln("Next(),will add task again")
		pTask.ToRunAfter = pTask.Interval
		wheel.Add(pTask)
	} else {
	}
	return true
}

func TestWheeler(t *testing.T) {

	trace := timewheel.NewTrace(0x222)

	periodTasks := []string{
		"2s", "12s", "12s", "12s", "22s", "32s", "42s", "3s", "4s", "5s", "6s", "7s", // "1024s",
	}
	tasks := make([]wheel.Runner, len(periodTasks))
	endDuration := time.Minute * 3

	w := wheel.NewTimeWheel("1s", 10, 5)
	w.Start()

	for idx, p := range periodTasks {
		interval, _ := time.ParseDuration(p)
		tasks[idx] = NewPeriodSleepTask(int64(trace.GetID()), interval, time.Now().Add(endDuration))
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
