package wheel

import (
	"sync"
	"time"
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
	PeriodTask
)

type RunnerResult interface{}

type Serializer interface {
	ToJson() string
}

// todo 任务对外接口，实现timeout机制
type Runner interface {
	//Serializer
	Run() (bool, error)
	// 执行完成之后的操作，如果是一次性任务，直接返回，如果是周期性任务，启动新goroutine执行，并再次添加到wheeler中，
	Next(wheel Wheeler) bool
	//预计执行runner的时刻
	GetToRunAt() time.Time
	GetToRunAfter() time.Duration
	// 真正运行的时间
	GetRunAt() time.Time
	// 返回状态是否运行结束
	IsDone() bool
	GetTimeout() time.Duration
	GetName() string
	GetID() int64
	SetError(error)
	// 返回运行结果
	Result() interface{}
}

// 任务的基本属性
type Task struct {
	//id，标识一个明确的请求，用于追踪
	ID int64  `json:"id,string"`
	// 名称，用于打印
	Name string `json:"name"`
	// 预计第一次运行时刻
	ToRunAt time.Time `json:"torunat,int64"`
	// 收到之后判断
	ToRunAfter time.Duration `json:"torunafter"`
	// 耗时，如果还没有执行 为0
	Cost time.Duration `json:"cost"`
	// 是否已经运行完成, 可以使用sync.Atomic来修改
	Done uint8 `json:"done"`
	// 错误描述
	err error `json:"-"`
	// timeout 时间
	Timeout time.Duration `json:"timeout,int64"`
	// 实际运行的时间
	RunAt time.Time `json:"runat,int64"`
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
	return t.ToRunAfter
}
func (t *Task) GetToRunAt() time.Time {
	return t.ToRunAt
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
	return t.RunAt
}
func (t *Task) GetTimeout() time.Duration {
	return t.Timeout
}

// 周期性的任务
type PeriodicTask struct {
	// 第一次运行的时间
	Task
	// 把触发时间+Period 就是下次执行的时间
	Interval time.Duration
	// 结束时间
	EndTime time.Time
}

func (t *PeriodicTask) ToJson() string {
	return ""
}

type Creator func(kv map[string]interface{}) Runner

type Factory interface {
	Register(name string, creator Creator)
	Create(kv map[string]interface{}) Runner
}

type taskFactory struct {
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
func (f *taskFactory) Create(kv map[string]interface{}) Runner {
	v, ok := kv["Name"]
	if ok {
		name := v.(string)
		if creator, existed := f.creatorMap[name]; existed {
			return creator(kv)
		} else {
			return nil
		}
	}
	return nil
}
