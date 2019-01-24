# delaytask-go

实现一个分布式的延时任务，通过redis实现任务的订阅和缓存；以及宕机后的现场恢复；瞬间收到大量任务（实测50000）时能够及时处理，
进程最高占用内存~=12M；执行结束后内存大约为8M

支持2种任务：
1. 定时任务，执行一次；
2. 周期性任务


## 任务的实现
>自定义任务需要包含wheel.Task，并且实现Run()和ToJson()方法
```go
type OncePingTask struct {
	delaytask.Task
	Url string `json:"Url"`
}

func (t *OncePingTask) Run() (bool, error) {
	resp, err := http.Get(t.Url)
	if err != nil {
		return false, err
	}
	t.RunAt = delaytask.TaskTime(time.Now())
	delaytask.Logger.WithFields(logrus.Fields{
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

```
## 周期性任务
>包含wheel.PeriodicTask，设置执行的结束时间和周期interval

```go
type PeriodPingTask struct {
	delaytask.PeriodicTask
	Url string `json:"Url"`
}

func (t *PeriodPingTask) Run() (bool, error) {
	resp, err := http.Get(t.Url)
	defer resp.Body.Close()
	if err != nil {
		return false, err
	}
	ioutil.ReadAll(resp.Body)
	delaytask.Logger.WithFields(logrus.Fields{
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
```


## 创建engine
```go
    engine := delaytask.NewEngine("1s", 10, "redis://:uestc12345@127.0.0.1:6379/4",
		"messageQ", "remote-task0:")
	engine.AddTaskCreator("OncePingTask", func(task string) delaytask.Runner {
		p := &OncePingTask{}
		if err := json.Unmarshal([]byte(task), p); err != nil {
		} else {
			return p
		}
		return nil
	})
	engine.AddTaskCreator("PeriodPingTask", func(task string) delaytask.Runner {
		t := &PeriodPingTask{}
		if err := json.Unmarshal([]byte(task), t); err != nil {
			return nil
		} else {
			return t
		}
	})
engine.Start()

```

## worker数量

>目前task的go routine pool中worker数量默认为cpu核数，可以根据自身任务的特点进行修改；如果偏重io的任务，可以适当增加worker的数量


## 客户端代码
```python
base_time = int(time.time() + 30)


def construct_once_task():
    # delay to run
    base_id = 1000000000000000
    random.seed(time.time())
    base_id += random.randint(100000,999999999)
    print("start",base_id)
    def generate_body():
        random.seed(time.time())
        second = random.randint(0, 1)
        to_run_at = base_time + second
        to_run_str = str(to_run_at)
        nonlocal base_id
        base_id += 1

        d =  {
            "ID": str(base_id),
            "Name": "OncePingTask",
            "ToRunAt": to_run_str,
            "ToRunAfter": "10",
            "Timeout": "1",
            "Url": "http://www.baidu.com"
        }
        return json.dumps(d)
    return generate_body

def construct_period_task():
    base_id = 2000000000000000
    random.seed(time.time())
    base_id += random.randint(100000,999999999)
    print("start",base_id)

    def generate_body():
        random.seed(time.time())
        second = random.randint(0, 10)
        to_run_at = base_time + second
        to_run_str = str(to_run_at)
        end_time = base_time + 600
        end_time_str = str(end_time)
        nonlocal base_id
        base_id += 1

        d =  {
            "ID": str(base_id),
            "Name": "PeriodPingTask",
            "ToRunAt": to_run_str,
            "Timeout": "1", 
            "Interval":"60", # 每分钟运行
            "EndTime":end_time_str,
            "Url": "http://www.baidu.com"
        }
        return json.dumps(d)
    return generate_body


def send_task():
    conn = redis.from_url(url="redis://:uestc12345@127.0.0.1:6379",db=4)
    # p = conn.pubsub(conn)
    generator_once = construct_once_task()
    generator_period = construct_period_task()
    for i in range(1):
        conn.publish("remote-task0:messageQ",generator_period())
    for i in range(0):
        conn.publish("remote-task0:messageQ",generator_once())
```
## go实例化task
```go
    tracer := trace.NewTrace(0x222)
	runInterval := time.Second * 50
	toRunAt := time.Now().Add(time.Minute * 2)
	t := &PeriodPingTask{
		PeriodicTask: delaytask.PeriodicTask{
			Task: delaytask.Task{
				ID:      tracer.GetID().Int64(),
				Name:    "PeriodPingTask",
				ToRunAt: delaytask.TaskTime(toRunAt),
				Done:    0,
				Timeout: delaytask.TaskDuration(time.Second * 5),
			},
			Interval: delaytask.TaskDuration(runInterval),
			EndTime:  delaytask.TaskTime(time.Now().Add(time.Hour * 24 * 365)),
		},
		Url: "http://www.baidu.com",
	}
```

## 设计思路以及测试方面
https://www.jianshu.com/p/805dbb5c9ac8

