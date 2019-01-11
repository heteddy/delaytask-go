package main

import (
	"timewheel/wheel"
	"time"
	"utils/trace"
	"net/http"
	"fmt"
	"io/ioutil"
	"timeService"
	"encoding/json"
)

type ServicePingTask struct {
	wheel.PeriodicTask
	Url string `json:"Url"`
}

func (t *ServicePingTask) Run() (bool, error) {
	fmt.Println("task run at", time.Now())
	resp, err := http.Get(t.Url)
	if err != nil {
		// 生成报警
		fmt.Println("告警发生")
		return false, err
	}
	body, _ := ioutil.ReadAll(resp.Body)
	fmt.Println("resp", resp.Status, resp.Body, string(body), time.Now())
	return true, nil
}

func (t *ServicePingTask) Next(w wheel.Wheeler) bool {
	now := time.Now()
	if now.After(time.Time(t.EndTime)) {
		return true
	} else {
		// 修改下次运行的时间
		t.ToRunAt = wheel.TaskTime(time.Now().Add(time.Duration(t.Interval)))
		w.Add(t)
	}
	return true
}

func main() {
	tracer := trace.NewTrace(0x222)
	runInterval := time.Second * 10
	task := &ServicePingTask{
		PeriodicTask: wheel.PeriodicTask{
			Task: wheel.Task{
				ID:         tracer.GetID().Int64(),
				Name:       "ServicePingTask",
				ToRunAt:    wheel.TaskTime(time.Now().Add(runInterval)),
				ToRunAfter: wheel.TaskDuration(runInterval),
				Done:       0,
				Timeout:    wheel.TaskDuration(time.Second * 5),
			},
			Interval: wheel.TaskDuration(runInterval),
			EndTime:  wheel.TaskTime(time.Now().Add(time.Hour * 24 * 365)),
		},
		Url: "http://101.132.72.222:8080/ping/",
	}
	if data, err := json.Marshal(task); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("json data", string(data))
		var s ServicePingTask
		if err := json.Unmarshal(data, &s); err != nil {
			fmt.Println(err)
		} else {
			fmt.Println(s.GetID(), s.GetName(), s.Url, s.ToRunAt.ToTime(), s.ToRunAfter.ToDuration())
		}
	}

	tw := wheel.NewTimeWheel("1s", 10, 5)
	tw.Add(task)
	tw.Start()
	// 最后启动timeService，否则导致不准
	timeService.TimerService.Start()
	select {}
	tw.Stop()
	timeService.TimerService.Stop()
}
