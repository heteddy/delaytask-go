package main

import (
	"timewheel/wheel"
	"net/http"
	"encoding/json"
	_ "net/http/pprof"
	"timewheel"
	"time"
	"wheelLogger"
	"github.com/sirupsen/logrus"
)

type OncePingTask struct {
	wheel.Task
	Url string `json:"Url"`
}

func (t *OncePingTask) Run() (bool, error) {
	resp, err := http.Get(t.Url)
	if err != nil {
		return false, err
	}
	t.RunAt = wheel.TaskTime(time.Now())
	wheelLogger.Logger.WithFields(logrus.Fields{
		"id":      t.GetID(),
		"RunAt":   t.GetRunAt(),
		"ToRunAt": t.GetToRunAt(),
	}).Infoln("OncePingTask ToRunAt RunAt")

	defer resp.Body.Close()
	wheelLogger.Logger.WithFields(logrus.Fields{
		"id":   t.GetID(),
	}).Infoln("ServicePingTask Run")
	return true, nil
}

func (t *OncePingTask) Next() bool {
	return true
}

func main() {
	engine := timewheel.NewEngine("1s", 10, "redis://:uestc12345@127.0.0.1:6379/4",
		"messageQ", "remote-task0:")
	engine.AddTaskCreator("OncePingTask", func(task string) wheel.Runner {
		p := &OncePingTask{}
		if err := json.Unmarshal([]byte(task), p); err != nil {
		} else {
			return p
		}
		return nil
	})

	engine.Start()

	http.ListenAndServe("0.0.0.0:6060", nil)
	select {}
	engine.Stop()
}
