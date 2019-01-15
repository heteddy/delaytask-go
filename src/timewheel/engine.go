package timewheel

import (
	"timeService"
	"timewheel/wheel"
	"time"
	"context"
	"sync"
	"timewheel/tracker"
	"wheelLogger"
	"github.com/sirupsen/logrus"
)

type DelayTaskEngine struct {
	timeWheel *wheel.TimeWheeler
	Storage   *TaskStorageService

	factory   wheel.Factory
	threshold time.Duration
	eventChan chan tracker.Event
	wg        sync.WaitGroup
	quit      chan bool
}

func (engine *DelayTaskEngine) createTask(task string) wheel.Runner {
	return engine.factory.Create(task)
}

func (engine *DelayTaskEngine) AddTaskCreator(name string, creator wheel.Creator) {
	engine.factory.Register(name, creator)
}

func (engine *DelayTaskEngine) LoadOngoingTask() {
	ongoingTask := &tracker.TaskLoadOngoing{}
	engine.eventChan <- ongoingTask
}

func (engine *DelayTaskEngine) EventOccur() {
	// 时间到通知，从waitingQ中获取
	e := &tracker.TaskLoadingEvent{}
	engine.eventChan <- e
}

func (engine *DelayTaskEngine) addTask(t string) bool {
	toRunAt, ok := engine.Storage.GetTaskToRunAt(t)
	//toRunAtTime := time.Time(toRunAt*1e9)
	toRunAtTime := time.Unix(toRunAt, 0)
	thresholdTime := time.Now().Add(engine.threshold)
	if ok {
		if toRunAtTime.After(thresholdTime) {
			// 放入WaitingQ
			wheelLogger.Logger.WithFields(logrus.Fields{
				"toRunAtTime": toRunAtTime,
				"threshold":   thresholdTime,
			}).Infoln("DelayTaskEngine add task,will add to waitingQ")
			engine.Storage.InsertToWaitingQ(t)
			engine.Storage.AppendToTaskTable(t)
		} else {
			// 创建task 放入timewheel
			wheelLogger.Logger.WithFields(logrus.Fields{
				"toRunAtTime": toRunAtTime,
				"threshold":   thresholdTime,
			}).Infoln("DelayTaskEngine add task,will add to timewheel")
			task := engine.createTask(t)
			engine.timeWheel.Add(task)
		}
		return true
	} else {
		return false
	}
}

func (engine *DelayTaskEngine) add(t string) {
	engine.addTask(t)
}
func (engine *DelayTaskEngine) remove(taskID string) {
	//taskID, _ := engine.Storage.GetTaskID(t)
	engine.Storage.ChangeTaskToComplete(taskID)
}

func (engine *DelayTaskEngine) Start() {
	/*
	1.创建timewheel,
	NewTimeWheel
	2.创建timeservice,
	3.创建storage service，添加回调，自动取出新发布的task，如果已经< threshold 发布到ongoingQ并添加到timewheel
		如果> threshold则放入waitingQ，

	4.创建创建定时器回调，定时从waitingQ中获取task放入ongoingQ，并且添加到timewheel中
	5.task 触发运行，触发回调函数把task从ongoing中移除；
	*/
	engine.timeWheel.Start()
	engine.Storage.Start()
	timeService.TimerService.Start()
	go func() {
		engine.wg.Add(1)
	loop:
		for {
			select {
			case event := <-engine.eventChan:
				taskType := event.GetType()
				switch taskType {
				case tracker.TASK_COMPLETE:
					engine.remove(event.GetTaskID())
				case tracker.TASK_Add:
					engine.add(event.GetTask())
				case tracker.TASK_RECEIVED:
					engine.onMessage(event.GetTask())
				case tracker.TASK_LOAD_ONGOING:
					taskStr, err := engine.Storage.LoadOngoingTask()
					if err != nil {
						wheelLogger.Logger.WithFields(logrus.Fields{
							"task": taskStr,
							"err":  err,
						}).Errorln("load ongoing task err")
					} else {
						for _, ts := range taskStr {
							//taskMap, err := engine.Storage.Deserialize(ts)
							task := engine.createTask(ts)
							if task != nil {
								wheelLogger.Logger.WithFields(logrus.Fields{
									"taskID":   task.GetID(),
									"taskName": task.GetName(),
								}).Infoln("DelayTaskEngine start:TASK_LOAD_ONGOING:create task success")
								engine.timeWheel.Add(task)
							} else {
								wheelLogger.Logger.WithFields(logrus.Fields{
									"taskStr": taskStr,
									"task":    task,
								}).Errorln("DelayTaskEngine start:ASK_LOAD_ONGOING:create task error")
							}
						}
					}
				case tracker.TASK_LOADING:
					taskStr, err := engine.Storage.MoveWaitingToOngoingQ(engine.threshold)
					if err != nil {
					} else {
						for _, ts := range taskStr {
							task := engine.createTask(ts)
							if task != nil {
								wheelLogger.Logger.WithFields(logrus.Fields{
									"taskID":   task.GetID(),
									"taskName": task.GetName(),
								}).Infoln("DelayTaskEngine start:TASK_LOADING:create task success")
								engine.timeWheel.Add(task)
							} else {
								wheelLogger.Logger.WithFields(logrus.Fields{
									"taskStr": taskStr,
									"task":    task,
								}).Errorln("DelayTaskEngine start:TASK_LOADING:create task error")
							}
						}
					}
				default:
				}
			case <-engine.quit:
				break loop
			}
		}
		engine.wg.Done()
	}()
	// engine 启动之后 载入ongoing task
	engine.LoadOngoingTask()
}

func (engine *DelayTaskEngine) Stop() {
	timeService.TimerService.Stop()
	engine.Storage.Stop()
	engine.timeWheel.Stop()
	engine.quit <- true
	engine.wg.Wait()
}

func (engine *DelayTaskEngine) HandleEvent(event tracker.Event) {
	engine.eventChan <- event
}

func (engine *DelayTaskEngine) onMessage(message string) bool {
	// 转化为map，然后提取toRunAt，执行分发
	// 收到消息转化为map，然后将map中的torunat取出来，
	// 根据threshold将决定放入waitingQ还是直接创建task
	return engine.addTask(message)
}

func NewEngine(duration string, slot int, subscribeUrl string, subscribeTopic string,
	prefix string) *DelayTaskEngine {
	tw := wheel.NewTimeWheel(duration, slot)

	ctx, _ := context.WithCancel(context.Background())
	s := NewTaskStorageService(ctx, subscribeUrl,
		time.Minute, subscribeTopic, prefix)
	// 3 倍的
	dur := time.Duration(int64(tw.RoundDuration()) * 3)
	wheelLogger.Logger.WithFields(logrus.Fields{
		"threshold":     dur,
	}).Infoln("NewEngine set threshold")


	engine := &DelayTaskEngine{
		timeWheel: tw,
		Storage:   s,

		factory:   wheel.NewTaskFactory(),
		threshold: dur,
		eventChan: make(chan tracker.Event, 5),
		wg:        sync.WaitGroup{},
		quit:      make(chan bool),
	}
	// 没一个round，取一次待执行的任务，保证每次取回来的任务round都是2
	timeService.TimerService.GetTimer(tw.RoundDuration().String()).Register(engine)
	tracker.Tracker.Subscribe(tracker.TASK_Add, engine)
	tracker.Tracker.Subscribe(tracker.TASK_COMPLETE, engine)
	tracker.Tracker.Subscribe(tracker.TASK_RECEIVED, engine)

	return engine
}
