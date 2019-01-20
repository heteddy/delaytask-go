package timewheel

import (
	"context"
	"github.com/gomodule/redigo/redis"
	"sync"
	"time"
	"strconv"
	"wheelLogger"
	"github.com/sirupsen/logrus"
	"timewheel/tracker"
	"encoding/json"
	"errors"
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
	taskTable string
	waitingQ  string
	ongoingQ  string
	quitChan  chan bool
	subCon    *redis.PubSubConn
	pool      *redis.Pool
	topic     string
	wg        sync.WaitGroup
	ctx       context.Context
}

func (service *TaskStorageService) setUpConnection() {
	service.subCon = &redis.PubSubConn{Conn: service.pool.Get()}
	service.subCon.Subscribe(service.topic)
}

func (service *TaskStorageService) startReceive() {
	service.wg.Add(1)
	/*
	for {
		// Get a connection from a pool
		c := pool.Get()
		psc := redis.PubSubConn{c}
		// Set up subscriptions
		psc.Subscribe("service.topic"))

		// While not a permanent error on the connection.
		for c.Err() == nil {
			switch v := psc.Receive().(type) {
			case redis.Message:

			case redis.Subscription:

			case error:

			}
		}
		c.Close()
	}
	*/

	go func() {
	loop:
		for {
			data := service.subCon.Receive()
			switch n := data.(type) {
			case error:
				// 重新订阅
				service.setUpConnection()
			case redis.Message:
				wheelLogger.Logger.WithFields(logrus.Fields{
					"channel": n.Channel,
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
	service.startReceive()
	return true
}
func (service *TaskStorageService) Stop() bool {
	service.subCon.Unsubscribe(service.topic)
	service.quitChan <- true
	// 这里关闭可能导致
	service.wg.Wait()
	service.pool.Close()
	return true
}

// 直接从ongoingQ中取出所有的task
func (service *TaskStorageService) LoadOngoingTask() ([]string, error) {
	c := service.pool.Get()
	defer c.Close()
	// ongoingQ中获取task id
	reply, err := redis.Strings(c.Do("LRANGE", service.ongoingQ, 0, -1))
	return reply, err
}

// 从waitingQ中取出task，放入ongoingQ中task
func (service *TaskStorageService) MoveWaitingToOngoingQ(toRunAfter time.Duration) ([]string, error) {

	// 1. 获取所有的小于 toRunAfter的
	now := time.Now()
	fromSec := now.Unix()
	toSec := now.Add(toRunAfter).Unix()
	// 根据时间 （< toRunAfter） 获取waitingQ中的task，
	c := service.pool.Get()
	defer c.Close()
	reply, err := redis.Strings(c.Do("ZRANGEBYSCORE", service.waitingQ, fromSec, toSec))
	if err != nil {
		wheelLogger.Logger.WithFields(logrus.Fields{
			"fromSec": fromSec,
			"toSec":   toSec,
			"err":     err,
		}).Warnln("TaskStorageService MoveWaitingToOngoingQ ZRANGEBYSCORE error")
	} else {
		wheelLogger.Logger.WithFields(logrus.Fields{
			"fromSec": fromSec,
			"toSec":   toSec,
		}).Warnln("load waiting ok")
	}
	if len(reply) > 0 {
		ongoingArg := make([]interface{}, len(reply)+1)
		ongoingArg[0] = service.ongoingQ

		for idx, t := range reply {
			ongoingArg[idx+1] = t
		}
		// task 直接放入ongoingQ
		ongoingReply, ongoingErr := c.Do("LPUSH", ongoingArg...)
		if ongoingErr != nil {
			wheelLogger.Logger.WithFields(logrus.Fields{
				"ongoingArg": ongoingArg,
				"reply":      ongoingReply,
				"ongoingErr": ongoingErr,
			}).Warnln("LPUSH error")
		}
		remReply, remErr := c.Do("ZREMRANGEBYSCORE", service.waitingQ, fromSec, toSec)
		if remErr != nil {
			wheelLogger.Logger.WithFields(logrus.Fields{
				"remReply": remReply,
				"remErr":   remErr,
			}).Warnln("ZREMRANGEBYSCORE error")
			return make([]string, 0), errors.New("remove waiting task error")
		}
	}
	return reply, err
}

// 从ongoingQ中删除task
func (service *TaskStorageService) ChangeTaskToComplete(tid string) {

	task, ok := service.GetTaskInfo(tid)
	if ok {
		c := service.pool.Get()
		defer c.Close()
		//1. 从task table中删除
		service.RemoveFromTaskTable(tid)
		// 2. remove from ongoing task
		reply, err := c.Do("LREM", service.ongoingQ, 0, task)
		if err != nil {
			wheelLogger.Logger.WithFields(logrus.Fields{
				"reply": reply,
				"err":   err,
			}).Warnln("Lrem ongoingQ error")
		} else {
			wheelLogger.Logger.WithFields(logrus.Fields{
			}).Warnln("ChangeTaskToComplete ok")
		}
	} else {
		//没有找到task
		wheelLogger.Logger.WithFields(logrus.Fields{
			"task": task,
			"ok":   ok,
		}).Warnln("not found task error")
	}

}
func (service *TaskStorageService) RemoveFromTaskTable(tid string) bool {
	c := service.pool.Get()
	defer c.Close()
	reply, err := c.Do("HDEL", service.taskTable, tid)

	if err != nil {
		wheelLogger.Logger.WithFields(logrus.Fields{
			"reply": reply,
			"err":   err,
		}).Infoln("TaskStorageService RemoveFromTaskTable fail")
		return false
	} else {
		wheelLogger.Logger.WithFields(logrus.Fields{
			"reply": reply,
			"err":   err,
		}).Infoln("TaskStorageService RemoveFromTaskTable okay")
	}
	return true
}

// 每个period task执行完成之后，可能会重新加入到waitingQ
//
func (service *TaskStorageService) InsertToWaitingQ(t string) bool {
	// 把 task 放到 waitingQ中
	toRunAt, ok := service.GetTaskToRunAt(t)
	if !ok {
		wheelLogger.Logger.WithFields(logrus.Fields{
			"toRunAt": toRunAt,
			"ok":      ok,
		}).Errorln("GetTaskToRunAt error")
		return false
	}
	c := service.pool.Get()
	defer c.Close()
	// task 放入 sorted waitingQ
	reply, err := c.Do("ZADD", service.waitingQ, toRunAt, t)
	if err != nil {
		wheelLogger.Logger.WithFields(logrus.Fields{
			"reply": reply,
			"err":   err,
		}).Errorln("ZADD waitingQ error")
		return false
	}
	return true
}
func (service *TaskStorageService) AppendToTaskTable(t string) bool {
	taskId, ok := service.GetTaskID(t)
	if !ok {
		wheelLogger.Logger.WithFields(logrus.Fields{
			"taskId": taskId,
			"ok":     ok,
		}).Errorln("GetTaskID error")
		return false
	}
	c := service.pool.Get()
	defer c.Close()
	// 插入到task table中h
	reply, err := c.Do("HSET", service.taskTable, taskId, t)
	if err != nil {
		wheelLogger.Logger.WithFields(logrus.Fields{
			"reply": reply,
			"err":   err,
		}).Errorln("HSET taskTable error")
		return false
	}

	return true
}
func (service *TaskStorageService) GetTaskInfo(tid string) (string, bool) {
	c := service.pool.Get()
	defer c.Close()
	// 插入到task table中h
	reply, err := redis.String(c.Do("HGET", service.taskTable, tid))

	if err != nil {
		wheelLogger.Logger.WithFields(logrus.Fields{
			"reply": reply,
			"err":   err,
		}).Errorln("GetTaskInfo error")
		return "", false
	}
	return reply, true
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
	c := service.pool.Get()
	defer c.Close()
	c.Do("PUBLISH", service.topic, t)
}

func NewTaskStorageService(ctx context.Context, url string, keepalive time.Duration, topic string,
	namePrefix string) *TaskStorageService {

	pool := &redis.Pool{
		Dial: func() (redis.Conn, error) {
			conn, err := redis.DialURL(url, redis.DialReadTimeout(10*time.Second),
				redis.DialWriteTimeout(10*time.Second), redis.DialConnectTimeout(10*time.Second),
			)
			if err != nil {
				return nil, err
			}
			return conn, err
		},
		MaxIdle:     5,
		IdleTimeout: 300 * time.Second,
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			if err != nil {
				//
			}
			return err
		},
	}

	t := &TaskStorageService{
		pool:      pool,
		wg:        sync.WaitGroup{},
		ctx:       ctx,
		topic:     namePrefix + topic,
		waitingQ:  namePrefix + "waitingSortedSet", // 保存 ToRunAt task
		ongoingQ:  namePrefix + "ongoingSet",       // 保存 task list
		taskTable: namePrefix + "allTasksHash",     // 保存taskid + serialized task
		quitChan:  make(chan bool, 1),
	}

	t.setUpConnection()
	return t
}
