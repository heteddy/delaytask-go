package main

import (
	"timewheel/wheel"
	"time"
	"utils/trace"
	"net/http"
	"fmt"
	"io/ioutil"
)

type ServicePingTask struct {
	wheel.PeriodicTask
	url string
}

func (t *ServicePingTask) Run() (bool, error) {
	resp, err := http.Get(t.url)
	if err != nil {
		// 生成报警
		fmt.Println("告警发生")
	}
	body, _ := ioutil.ReadAll(resp.Body)
	fmt.Println("resp", resp.Status, resp.Body, string(body))
	return true, nil
}

func (t *ServicePingTask) Next(w wheel.Wheeler) bool {
	now := time.Now()
	if now.After(t.EndTime) {
		return true
	} else {
		// 修改下次运行的时间
		t.ToRunAt = time.Now().Add(t.Interval)
		w.Add(t)
	}
	return true
}

func main() {
	tracer := trace.NewTrace(0x222)
	/*
	ID int64
	// 类型
	TaskType uint8
	// 名称，用于打印
	Name string
	// 预计第一次运行时刻
	ToRunAt time.Time
	// 收到之后判断
	ToRunAfter time.Duration
	// 耗时，如果还没有执行 为0
	Cost time.Duration
	// 是否已经运行完成, 可以使用sync.Atomic来修改
	Done uint8
	// 错误描述
	err error
	// timeout 时间
	Timeout time.Duration
	// 实际运行的时间
	RunAt time.Time
	*/
	runInterval := time.Second * 10
	task := &ServicePingTask{
		PeriodicTask: wheel.PeriodicTask{
			Task: wheel.Task{
				ID:         tracer.GetID().Int64(),
				Name:       "ServicePingTask",
				ToRunAt:    time.Now().Add(runInterval),
				ToRunAfter: runInterval,
				Done:       0,
				Timeout:    time.Second * 5,
			},
			Interval: runInterval,
			EndTime:  time.Now().Add(time.Hour * 24 * 365),
		},
		url: "http://127.0.0.1:8080/ping/",
	}

	tw := wheel.NewTimeWheel("1s", 10, 5)
	tw.Add(task)
	tw.Start()
	select {}
	tw.Stop()
}
