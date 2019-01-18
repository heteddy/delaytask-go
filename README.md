# delaytask-go

实现一个分布式的延时任务，通过redis实现任务的订阅和缓存；以及宕机后的现场恢复；瞬间收到大量任务（实测50000）时能够及时处理，
进程最高占用内存~=12M；执行结束后内存大约为8M

支持2种任务：
1. 定时任务，执行一次；
2. 周期性任务

#例子
任务的实现，自定义任务需要包含wheel.Task，并且实现Run()和Next()方法
一次性任务的next 直接返回即可
```go
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
	defer resp.Body.Close()
	return true, nil
}

func (t *OncePingTask) Next() bool {
	return true
}
```
周期性任务，有以下2点非常重要，
1. 判断任务是否已经达到定时结束时间，比如定时任务每10分钟执行一次；一直到2019年1月1日；2019-1-1就是EndTime
2. 修改下次toRunAt的时间，
3. 发布add task event
```go
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

	return true, nil
}

func (t *ServicePingTask) Next() bool {
	now := time.Now()

	if now.After(t.EndTime.ToTime()) {
		return true
	} else {
		// 修改下次运行的时间
		t.ToRunAt = wheel.TaskTime(time.Now().Add(t.Interval.ToDuration()))
		tStr, _ := json.Marshal(t)

		tracker.Tracker.Publish(&tracker.TaskAddEvent{string(tStr)})
	}
	return true
}
```


创建engine
```go

    engine := timewheel.NewEngine("1s", 10, "redis://:uestc12345@127.0.0.1:6379/4",
		"messageQ", "remote-task0:")
    // 通过task的名称，创建任务；
	engine.AddTaskCreator("OncePingTask", func(task string) wheel.Runner {
		p := &OncePingTask{}
		if err := json.Unmarshal([]byte(task), p); err != nil {
		} else {
			return p
		}
		return nil
	})

	engine.Start()

	select {}
	engine.Stop()

```

# worker数量
目前task的go routine poll中worker数量默认为cpu核数，可以根据自身任务的特点进行修改；如果偏重io的任务，可以适当增加worker的数量



设计思路以及测试方面

https://www.jianshu.com/p/805dbb5c9ac8