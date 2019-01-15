package main

import (
	"timewheel/wheel"
	"time"
	"utils/trace"
	"net/http"
	"io/ioutil"
	"encoding/json"

	"timewheel/tracker"
	"timewheel"
	"wheelLogger"
	"github.com/sirupsen/logrus"
	_ "net/http/pprof"
	"log"
)

type ServicePingTask struct {
	wheel.PeriodicTask
	Url string `json:"Url"`
}

func (t *ServicePingTask) Run() (bool, error) {

	resp, err := http.Get(t.Url)
	if err != nil {
		return false, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	wheelLogger.Logger.WithFields(logrus.Fields{
		"id":   t.GetID(),
		"body": string(body),
		"err":  err,
	}).Infoln("ServicePingTask Run")
	return true, nil
}

func (t *ServicePingTask) Next() bool {
	now := time.Now()

	wheelLogger.Logger.WithFields(logrus.Fields{
		"now":     now,
		"EndTime": t.EndTime.ToTime(),
	}).Infoln("ServicePingTask Next")
	if now.After(t.EndTime.ToTime()) {
		return true
	} else {
		// 修改下次运行的时间
		t.ToRunAt = wheel.TaskTime(time.Now().Add(t.Interval.ToDuration()))
		tStr, _ := json.Marshal(t)

		wheelLogger.Logger.WithFields(logrus.Fields{
			"ID":        t.GetID(),
			"t.ToRunAt": t.ToRunAt.ToTime(),
		}).Infoln("ServicePingTask Publish Next")
		tracker.Tracker.Publish(&tracker.TaskAddEvent{string(tStr)})
	}
	return true
}

func main() {
	tracer := trace.NewTrace(0x222)
	runInterval := time.Second * 50
	toRunAt := time.Now().Add(time.Minute * 2)
	task := &ServicePingTask{
		PeriodicTask: wheel.PeriodicTask{
			Task: wheel.Task{
				ID:         tracer.GetID().Int64(),
				Name:       "ServicePingTask",
				ToRunAt:    wheel.TaskTime(toRunAt),
				ToRunAfter: wheel.TaskDuration(runInterval),
				Done:       0,
				Timeout:    wheel.TaskDuration(time.Second * 5),
			},
			Interval: wheel.TaskDuration(runInterval),
			EndTime:  wheel.TaskTime(time.Now().Add(time.Hour * 24 * 365)),
		},
		Url: "http://101.132.72.222:8080/ping/",
	}

	engine := timewheel.NewEngine("1s", 10, "redis://:uestc12345@127.0.0.1:6379/4",
		"messageQ", "remote-task0:")
	engine.AddTaskCreator("ServicePingTask", func(task string) wheel.Runner {
		p := &ServicePingTask{}
		if err := json.Unmarshal([]byte(task), p); err != nil {

		} else {
			return p
		}
		return nil
	})

	engine.Start()

	if data, err := json.Marshal(task); err != nil {
	} else {
		time.Sleep(time.Second * 1)
		engine.Storage.Publish(string(data))
	}
	go func() {
		log.Println(http.ListenAndServe("localhost:10000", nil))
	}()
	select {}
	engine.Stop()
}
