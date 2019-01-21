package delaytask

import (
	"time"
	"strconv"
	"strings"
)

const (
	// 普通的延时任务,或者定时任务
	DelayTask = iota
	// 周期性任务
	PeriodTask
)

type Serializer interface {
	ToJson() string
}
type Runner interface {
	Serializer
	Run() (bool, error)
	// 计划执行runner的时刻
	GetToRunAt() time.Time
	UpdateToRunAt()
	GetRunAt() time.Time
	GetType() int
	IsTaskEnd() bool
	GetTimeout() time.Duration
	GetName() string
	GetID() int64
	SetError(error)
	// 返回运行结果
	Result() interface{}
}
type IWorker interface {
	Execute(Runner)
}

type TaskTime time.Time
type TaskDuration time.Duration

func (t TaskTime) ToTime() time.Time {
	return time.Time(t)
}
func (t TaskDuration) ToDuration() time.Duration {
	return time.Duration(t)
}

// 精确到秒全部转化为Unix 格式
func (t *TaskTime) UnmarshalJSON(data []byte) error {
	unixStr := string(data)
	unixStr = strings.Trim(unixStr, "\"")
	unix, err := strconv.ParseInt(unixStr, 10, 64)
	if err != nil {
		return err
	}
	*t = TaskTime(time.Unix(unix, 0))
	return nil
}

func (t TaskTime) MarshalJSON() ([]byte, error) {
	unix := time.Time(t).Unix()
	unixStr := strconv.FormatInt(unix, 10)
	unixStr = "\"" + unixStr + "\""
	return []byte(unixStr), nil
}
func (t *TaskDuration) UnmarshalJSON(data []byte) error {
	secondStr := string(data)
	secondStr = strings.Trim(secondStr, "\"")
	seconds, err := strconv.ParseInt(secondStr, 10, 64)
	if err != nil {
		return err
	}

	*t = TaskDuration(seconds * 1e9)

	return nil
}

func (t TaskDuration) MarshalJSON() ([]byte, error) {
	seconds := int64(time.Duration(t)) / 1e9
	secondStr := strconv.FormatInt(seconds, 10)
	secondStr = "\"" + secondStr + "\""
	return []byte(secondStr), nil
}

type Creator func(task string) Runner

type Factory interface {
	Register(name string, creator Creator)
	Create(string) Runner
}
