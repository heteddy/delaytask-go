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
	"io/ioutil"
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
	return true, nil
}

func (t *OncePingTask) ToJson() string {
	b, err := json.Marshal(t)
	if err != nil {
		return ""
	}
	return string(b)
}

type PeriodPingTask struct {
	wheel.PeriodicTask
	Url string `json:"Url"`
}

func (t *PeriodPingTask) Run() (bool, error) {
	resp, err := http.Get(t.Url)
	defer resp.Body.Close()
	if err != nil {
		return false, err
	}
	ioutil.ReadAll(resp.Body)
	wheelLogger.Logger.WithFields(logrus.Fields{
		"id":  t.GetID(),
		"err": err,
	}).Infoln("PeriodPingTask Run")
	return true, nil
}
func (t *PeriodPingTask) ToJson() string {
	b, err := json.Marshal(t)
	if err != nil {
		return ""
	}
	return string(b)
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
	engine.AddTaskCreator("PeriodPingTask", func(task string) wheel.Runner {
		t := &PeriodPingTask{}
		if err := json.Unmarshal([]byte(task), t); err != nil {
			return nil
		} else {
			return t
		}
	})
	//tracer := trace.NewTrace(0x222)
	//runInterval := time.Second * 50
	//toRunAt := time.Now().Add(time.Minute * 2)
	//t := &PeriodPingTask{
	//	PeriodicTask: wheel.PeriodicTask{
	//		Task: wheel.Task{
	//			ID:      tracer.GetID().Int64(),
	//			Name:    "PeriodPingTask",
	//			ToRunAt: wheel.TaskTime(toRunAt),
	//			Done:    0,
	//			Timeout: wheel.TaskDuration(time.Second * 5),
	//		},
	//		Interval: wheel.TaskDuration(runInterval),
	//		EndTime:  wheel.TaskTime(time.Now().Add(time.Hour * 24 * 365)),
	//	},
	//	Url: "http://www.baidu.com",
	//}
	engine.Start()

	http.ListenAndServe("0.0.0.0:6060", nil)
	select {}
	engine.Stop()
}
