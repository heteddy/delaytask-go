package timewheel

import (
	"context"
	"github.com/gomodule/redigo/redis"
	"sync"
	"time"
	"github.com/pkg/errors"
	"strconv"
	"timeService"
	"wheelLogger"
	"github.com/sirupsen/logrus"
	"timewheel/tracker"
	"encoding/json"
)

/*
1. 读取正在运行的tasks，场景：重新启动timewheel进程的时候，读取ongoing task，如果是异常崩溃，
   则可以从这里回复现场
2. 启动服务，服务启动之后，
	2.1 执行keepalive，
	2.2 向redis订阅序列化的task，
3.
*/
type StorageService interface {
	LoadOngoingTask() ([]string, error)
	AppendToTaskTable(string) bool
	GetTaskInfo(tid string) (string, bool)
	InsertToWaitingQ(string) bool
	MoveWaitingToOngoingQ(time.Duration) ([]string, error)
	ChangeTaskToComplete(string)
	RemoveFromTaskTable(string) bool

	Deserialize(string) (map[string]interface{}, error)
	Start() bool
	Stop() bool
}

type TaskStorageService struct {
	taskTable          string
	waitingQ           string
	ongoingQ           string
	keepaliveTimerChan chan bool
	keepalive          time.Duration
	connection         redis.Conn
	quitChan           chan bool
	subCon             redis.PubSubConn
	topic              string
	wg                 sync.WaitGroup
	ctx                context.Context
}

func (service *TaskStorageService) startKeepAlive() {
	service.wg.Add(1)
	go func() {
		//
		//ticker := time.NewTicker(service.keepalive)
		var err error
	loop:
		for err == nil {
			select {
			case <-service.keepaliveTimerChan:
				wheelLogger.Logger.WithFields(logrus.Fields{
					"time": time.Now(),
				}).Infoln("keep alive for subscriber!!")
				service.connection.Send("PING", "")
				if err = service.connection.Flush(); err != nil {
				}
				service.subCon.Ping("")
			case <-service.quitChan:
				break loop
			case <-service.ctx.Done():
				break loop
			}
		}

		wheelLogger.Logger.WithFields(logrus.Fields{
		}).Warnln("TaskStorageService exit startKeepalive!!")
		service.wg.Done()
	}()
}
func (service *TaskStorageService) EventOccur() {
	service.keepaliveTimerChan <- true
}
func (service *TaskStorageService) startReceive() {

	service.wg.Add(1)
	go func() {
	loop:
		for {
			data := service.subCon.Receive()
			switch n := data.(type) {
			case error:
				break loop
			case redis.Message:
				wheelLogger.Logger.WithFields(logrus.Fields{
					"channel": n.Channel,
					"data":    string(n.Data),
				}).Infoln("TaskStorageService receive data from redis")
				// 自动保存到taskTable
				tracker.Tracker.Publish(&tracker.TaskReceivedEvent{string(n.Data)})
			case redis.Subscription:
				switch n.Count {
				case 1:
				case 0:
					// 结束subscribe
					wheelLogger.Logger.WithFields(logrus.Fields{
					}).Warnln("unsubscribe will exit!!")
					break loop
				}
			}
		}
		wheelLogger.Logger.WithFields(logrus.Fields{
		}).Warnln("TaskStorageService exit receive!!")

		service.wg.Done()
	}()
}

func (service *TaskStorageService) Start() bool {
	service.startKeepAlive()
	service.startReceive()
	return true
}
func (service *TaskStorageService) Stop() bool {
	service.subCon.Unsubscribe(service.topic)
	service.quitChan <- true
	// 这里关闭可能导致
	service.wg.Wait()
	timeService.TimerService.GetTimer("1m").Unregister(service)
	close(service.keepaliveTimerChan)
	service.subCon.Close()
	service.connection.Close()
	return true
}

// 直接从ongoingQ中取出所有的task
func (service *TaskStorageService) LoadOngoingTask() ([]string, error) {
	// ongoingQ中获取task id
	reply, _ := service.connection.Do("LRANGE", service.ongoingQ, 0, -1)

	switch reply.(type) {
	case []interface{}:
		tasks := reply.([]interface{})
		if len(tasks) > 0 {
			result := make([]string, len(tasks))
			for idx, idInterface := range tasks {
				result[idx] = string(idInterface.([]byte))
			}
			return result, nil
		} else {
			// no ongoing tasks
			return make([]string, 0), nil
		}
	default:
	}
	return make([]string, 0), errors.New("no ongoing tasks")
}

// 从waitingQ中取出task，放入ongoingQ中task
func (service *TaskStorageService) MoveWaitingToOngoingQ(toRunAfter time.Duration) ([]string, error) {

	// 1. 获取所有的小于 toRunAfter的
	now := time.Now()
	fromSec := now.Unix()
	toSec := now.Add(toRunAfter).Unix()
	// 根据时间 （< toRunAfter） 获取waitingQ中的task，

	//wheelLogger.Logger.WithFields(logrus.Fields{
	//	"fromSec": fromSec,
	//	"toSec":   toSec,
	//}).Infoln("TaskStorageService MoveWaitingToOngoingQ")
	reply, err := service.connection.Do("ZRANGEBYSCORE", service.waitingQ, fromSec, toSec)
	if err != nil {
		wheelLogger.Logger.WithFields(logrus.Fields{
			"fromSec": fromSec,
			"toSec":   toSec,
			"err":     err,
		}).Warnln("TaskStorageService MoveWaitingToOngoingQ ZRANGEBYSCORE error")
	}
	// 2. 移动符合条件的task到ongoingQ
	switch reply.(type) {
	case []interface{}:
		waitingTasks := reply.([]interface{})
		if length := len(waitingTasks); length > 0 {
			result := make([]string, length)

			ongoingArg := make([]interface{}, length+1)
			ongoingArg[0] = service.ongoingQ

			for idx, t := range waitingTasks {
				result[idx] = string(t.([]byte))
				ongoingArg[idx+1] = t
			}
			// task 直接放入ongoingQ

			reply, err := service.connection.Do("LPUSH", ongoingArg...)
			if err != nil {
				wheelLogger.Logger.WithFields(logrus.Fields{
					"ongoingArg": ongoingArg,
					"reply":      reply,
					"err":        err,
				}).Warnln("TaskStorageService MoveWaitingToOngoingQ LPUSH to ongoingQ error")
			} else {
				// 移除waitingQ的内容
				reply, err = service.connection.Do("ZREMRANGEBYSCORE", service.waitingQ, fromSec, toSec)
			}
			return result, nil
		} else {
			return make([]string, 0), nil
		}
	}
	// 3. 返回符合条件的task
	return make([]string, 0), nil
}

// 从ongoingQ中删除task
func (service *TaskStorageService) ChangeTaskToComplete(tid string) {
	task, ok := service.GetTaskInfo(tid)
	if ok {
		// todo 需要使用pipline?
		//1. 从task table中删除
		// taskId, ok := service.getTaskID(t)
		service.RemoveFromTaskTable(tid)
		// 2. remove from ongoing task
		reply, err := service.connection.Do("LREM", service.ongoingQ, 0, task)
		if err!=nil {
			wheelLogger.Logger.WithFields(logrus.Fields{
				"reply": reply,
				"err":   err,
			}).Infoln("TaskStorageService ChangeTaskToComplete remove from ongoingQ")
		}

	} else {
		//没有找到task
		wheelLogger.Logger.WithFields(logrus.Fields{
			"task": task,
			"ok":   ok,
		}).Warnln("TaskStorageService ChangeTaskToComplete get task info error")
	}

}
func (service *TaskStorageService) RemoveFromTaskTable(tid string) bool {
	reply, err := service.connection.Do("HDEL", service.taskTable, tid)

	if err != nil {
		wheelLogger.Logger.WithFields(logrus.Fields{
			"reply": reply,
			"err":   err,
		}).Infoln("TaskStorageService RemoveFromTaskTable")
		return false
	}
	return true
}

// 每个period task执行完成之后，可能会重新加入到waitingQ
//
func (service *TaskStorageService) InsertToWaitingQ(t string) bool {
	// 把 task 放到 waitingQ中
	toRunAt, ok := service.GetTaskToRunAt(t)
	if !ok {
		return false
	}
	// task 放入 sorted waitingQ
	_, err := service.connection.Do("ZADD", service.waitingQ, toRunAt, t)
	if err != nil {
		return false
	}

	return true
}
func (service *TaskStorageService) AppendToTaskTable(t string) bool {
	taskId, ok := service.GetTaskID(t)
	if !ok {
		return false
	}
	// 插入到task table中h
	_, err := service.connection.Do("HSET", service.taskTable, taskId, t)
	if err != nil {
		return false
	}

	return true
}
func (service *TaskStorageService) GetTaskInfo(tid string) (string, bool) {

	// 插入到task table中h
	reply, err := service.connection.Do("HGET", service.taskTable, tid)

	if err != nil {
		return "", false
	}
	switch reply.(type) {
	case interface{}:
		result := string(reply.([]byte))
		return result, true
	default:
		return "", false
	}
}

func (service *TaskStorageService) Deserialize(t string) (map[string]interface{}, error) {

	taskMap := make(map[string]interface{})

	err := json.Unmarshal([]byte(t), &taskMap)
	if err != nil {
		wheelLogger.Logger.WithFields(logrus.Fields{
			"err": err,
		}).Errorln("TaskStorageService Deserialize error")
	} else {
	}
	return taskMap, nil
}

func (service *TaskStorageService) GetTaskID(t string) (taskid int64, ok bool) {
	taskMap, err := service.Deserialize(t)
	if err != nil {
		return 0, false
	}
	_taskId, found := taskMap["ID"]
	if !found {
		//todo log error
		wheelLogger.Logger.WithFields(logrus.Fields{
			"task": t,
		}).Errorln("TaskStorageService GetTaskID no ID in task")
		return 0, false
	}
	var _err error
	taskid, _err = strconv.ParseInt(_taskId.(string), 10, 64)
	if _err != nil {
	}
	return taskid, true
}
func (service *TaskStorageService) GetTaskToRunAt(t string) (int64, bool) {
	taskMap, err := service.Deserialize(t)
	if err != nil {
		return 0, false
	}

	taskToRun, found := taskMap["ToRunAt"]
	if !found {
		wheelLogger.Logger.WithFields(logrus.Fields{
		}).Errorln("GetTaskToRunAt fields error:ToRunAt")
		return 0, false
	}
	toRunAt, _ := strconv.ParseInt(taskToRun.(string), 10, 64)
	return toRunAt, true
}
func (service *TaskStorageService) Publish(t string) {
	service.connection.Do("PUBLISH", service.topic, t)
}

func NewTaskStorageService(ctx context.Context, url string, keepalive time.Duration, topic string,
	namePrefix string) *TaskStorageService {

	conn, err := redis.DialURL(url, redis.DialReadTimeout(keepalive+10*time.Second),
		redis.DialWriteTimeout(10*time.Second))
	if err != nil {
		// 打印log 并且退出
		return nil
	}
	// 当前连接订阅 topic
	subCon := redis.PubSubConn{conn}

	conn2, err := redis.DialURL(url, redis.DialReadTimeout(keepalive+10*time.Second),
		redis.DialWriteTimeout(10*time.Second))
	if err != nil {
		// 打印log 并且退出
		return nil
	}

	t := &TaskStorageService{
		connection:         conn2,
		subCon:             subCon,
		wg:                 sync.WaitGroup{},
		ctx:                ctx,
		keepaliveTimerChan: make(chan bool, 1),
		keepalive:          keepalive,
		topic:              namePrefix + topic,
		waitingQ:           namePrefix + "waitingSortedSet", // 保存 ToRunAt task
		ongoingQ:           namePrefix + "ongoingSet",       // 保存 task list
		taskTable:          namePrefix + "allTasksHash",     // 保存taskid + serialized task
		quitChan:           make(chan bool, 1),
	}
	// 当前连接已经订阅 topic 因此需要
	t.subCon.Subscribe(t.topic)
	timeService.TimerService.GetTimer("1m").Register(t)
	return t
}
