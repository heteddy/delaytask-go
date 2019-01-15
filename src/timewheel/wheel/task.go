package wheel

import (
	"sync"
	"time"
	"encoding/json"
	"strings"
	"wheelLogger"
	"github.com/sirupsen/logrus"
)

const (
	Created   = iota
	Queued
	Completed
)

const (
	// 普通的延时任务,或者定时任务
	FixedTime = iota
	// 周期性任务
	Period
)

type RunnerResult interface{}

type Serializer interface {
	ToJson() string
}

// todo 任务对外接口，实现timeout机制

// 定制task time的json，

// 任务的基本属性
type Task struct {
	// id，标识一个明确的请求，用于追踪
	ID int64 `json:",string"`
	// 名称，用于打印
	Name string `json:"Name"`
	// 预计第一次运行时刻
	ToRunAt TaskTime `json:"ToRunAt"`
	// 收到之后判断
	ToRunAfter TaskDuration `json:"ToRunAfter"`
	// 耗时，如果还没有执行 为0
	Cost TaskDuration `json:"-"`
	// 是否已经运行完成, 可以使用sync.Atomic来修改
	Done uint8 `json:"-"`
	// 错误描述, 小写开头字段(private)不能被导出
	err error `json:"-"`
	// timeout 时间
	Timeout TaskDuration `json:"Timeout"`
	// 实际运行的时间
	RunAt TaskTime `json:"-"`
}

//Runner.Result
func (t *Task) Result() interface{} {
	return ""
}
func (t *Task) ToJson() string {
	return ""
}
func (t *Task) SetError(err error) {
	t.err = err
}
func (t *Task) GetToRunAfter() time.Duration {
	return time.Duration(t.ToRunAfter)
}
func (t *Task) GetToRunAt() time.Time {
	return time.Time(t.ToRunAt)
}

func (t *Task) GetName() string {
	return t.Name
}
func (t *Task) GetID() int64 {
	return t.ID
}

func (t *Task) IsDone() bool {
	return t.Done == Completed
}

func (t *Task) GetRunAt() time.Time {
	return time.Time(t.RunAt)
}
func (t *Task) GetTimeout() time.Duration {
	return time.Duration(t.Timeout)
}

// 周期性的任务
type PeriodicTask struct {
	// 第一次运行的时间
	Task
	// 把触发时间+Period 就是下次执行的时间
	Interval TaskDuration `json:"Interval"`
	// 结束时间
	EndTime TaskTime `json:"EndTime"`
}

func (t *PeriodicTask) ToJson() string {
	return ""
}

type taskFactory struct {
	// 根据task name 创建一个具体的task
	creatorMap map[string]Creator
}

var once sync.Once
var pFactory *taskFactory

func NewTaskFactory() Factory {
	once.Do(func() {
		pFactory = &taskFactory{
			creatorMap: make(map[string]Creator),
		}
	})
	return pFactory
}

// 如果typeID重复，覆盖已有的creator
func (f *taskFactory) Register(name string, creator Creator) {
	if _, existed := f.creatorMap[name]; existed {
		// warning
	}
	f.creatorMap[name] = creator

}
func (f *taskFactory) Create(task string) Runner {
	taskMap := make(map[string]interface{})

	wheelLogger.Logger.WithFields(logrus.Fields{
	}).Infoln("task factory create task")
	if err := json.Unmarshal([]byte(task), &taskMap); err != nil {
		//todo log error
		wheelLogger.Logger.WithFields(logrus.Fields{
			"task": task,
			"err":  err,
		}).Errorln("taskFactory:Create:task Unmarshal error")
		return nil
	}

	v, ok := taskMap["Name"]
	if ok {
		name1 := v.(string)
		name := strings.Trim(name1, "\"")
		creator, existed := f.creatorMap[name]
		if existed {
			runner := creator(task)
			wheelLogger.Logger.WithFields(logrus.Fields{
				"id":         runner.GetID(),
				"name":       runner.GetName(),
				"ToRunAt":    runner.GetToRunAt(),
				"ToRunAfter": runner.GetToRunAfter(),
			}).Infoln("create okay")
			return runner
		} else {
			wheelLogger.Logger.WithFields(logrus.Fields{
				"task": task,
			}).Errorln("not found creator")
			return nil
		}
	}
	return nil
}
