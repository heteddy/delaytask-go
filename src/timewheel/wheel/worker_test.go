package wheel

import (
	"fmt"
	"testing"
	"time"
)

type TestWheeler struct {
}

func (w *TestWheeler) Add(runner Runner) {
	fmt.Println("Add runner", runner.GetID())
}
func (w *TestWheeler) Remove(runner int64) {
	fmt.Println("Remove runner", runner)
}

func (w *TestWheeler) ListRunner() {
	fmt.Println("ListRunner")
}

type PeriodSleepTask2 struct {
	PeriodicTask
}

func (p *PeriodSleepTask2) Run() (bool, error) {
	p.RunAt = time.Now()
	time.Sleep(time.Second * 1)
	p.Cost = time.Now().Sub(p.RunAt)
	fmt.Println("PeriodSleepTask run complete", p.ID)
	return true, nil
}
func (p *PeriodSleepTask2) Next(Wheeler) bool {
	fmt.Println("run next")
	return true
}
func NewPeriodSleepTask2(requestID int64, period time.Duration, end time.Time) *PeriodSleepTask2 {
	t := &PeriodSleepTask2{}

	t.ID = requestID
	t.Name = "PeriodSleepTask"
	//interval, _ := time.ParseDuration("3s")
	t.Interval = period
	t.ToRunAfter = period
	//runDuration, _ := time.ParseDuration("1m")
	t.EndTime = end
	fmt.Println("create task", "id:", t.ID, "Period:", t.Interval, "end:", t.EndTime)
	return t
}

func TestWorkerPool(t *testing.T) {
	taskChan := make(chan Runner, 1)
	pool := NewPool(5, taskChan, &TestWheeler{})
	pool.Start()

	periodTasks := []string{
		"3s", "4s", "5s", "6s", "7s", "12s",
	}
	duration := time.Minute * 3
	tasks := make([]Runner, len(periodTasks))
	for idx, p := range periodTasks {
		interval, _ := time.ParseDuration(p)
		tasks[idx] = NewPeriodSleepTask2(int64(idx), interval, time.Now().Add(duration))
	}
	for i := 0; i < len(periodTasks); i++ {
		pool.Execute(tasks[i])
	}
	time.Sleep(time.Second * 20)
	close(taskChan)
	pool.Stop()
	t.Log("worker ok")
}
