package timewheel

import (
	"context"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"sync"
	"time"
	"encoding/json"
	"github.com/pkg/errors"
	"strings"
	"strconv"
)

/*
1. 读取正在运行的tasks，场景：重新启动timewheel进程的时候，读取ongoing task，如果是异常崩溃，则可以从这里回复现场
2. 启动服务，服务启动之后，
	2.1 执行keepalive，
	2.2 向redis订阅序列化的task，
3.
*/
type StorageService interface {
	LoadOngoingTask() ([]string, error)
	AppendToTaskTable(string) bool
	InsertToWaitingQ(string) bool
	MoveWaitingToOngoingQ(time.Duration) ([]string, error)
	ChangeTaskToComplete(string, bool)
	RemoveFromTaskTable(int64) bool
	Start() bool
	Stop() bool
}

type TaskComingFunc func(channel string, message []byte) bool

type TaskStorageService struct {
	//client * RedisClient
	//subscriber *taskSubscriber
	taskTable          string
	waitingQ           string
	ongoingQ           string
	keepalive          time.Duration
	connection         redis.Conn
	quitChan           chan bool
	subCon             redis.PubSubConn
	topic              string
	wg                 sync.WaitGroup
	ctx                context.Context
	remoteTaskCallback TaskComingFunc
}


func (service *TaskStorageService) startKeepAlive() {
	service.wg.Add(1)
	go func() {
		ticker := time.NewTicker(service.keepalive)
		var err error
	loop:
		for err == nil {
			select {
			case <-ticker.C:
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
		fmt.Println("exit startKeepalive!!")
		service.wg.Done()
	}()
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
				fmt.Println("message", n)
				// 自动保存到taskTable
				service.remoteTaskCallback(n.Channel, n.Data)
			case redis.Subscription:
				switch n.Count {
				case 1:
				case 0:
					// 结束subscribe
					break loop
				}
			}
		}
		fmt.Println("exit receive!!")
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
	service.wg.Wait()

	service.subCon.Close()
	service.connection.Close()
	return true
}
func (service *TaskStorageService) LoadOngoingTask() ([]string, error) {
	result := make([]string, 0)
	reply, err := service.connection.Do("LRANGE", service.ongoingQ, 0, -1)
	fmt.Println(reply, err)
	return result, nil
}
func (service *TaskStorageService) MoveWaitingToOngoingQ(toRunAfter time.Duration) ([]string, error) {
	result := make([]string, 0)

	// 1. 获取所有的小于 toRunAfter的
	now := time.Now()
	fromSec := now.Unix()
	toSec := now.Add(toRunAfter).Unix()
	reply, err := service.connection.Do("ZRANGEBYSCORE", service.waitingQ, fromSec, toSec)
	fmt.Println(reply, err)
	// 2. 移动符合条件的task到ongoingQ

	// 3. 返回符合条件的task
	return result, nil
}
func (service *TaskStorageService) ChangeTaskToComplete(t string, removeFromTaskTable bool) {
	// 1. remove from ongoing task
	service.connection.Do("LREM", service.ongoingQ, 0, t)
	// 2. 从task table中删除

	if removeFromTaskTable {
		taskId, ok := service.getTaskID(t)
		if !ok {
			// todo: log error
			return
		}
		service.connection.Do("HDEL", service.taskTable, taskId)
	}
}
func (service *TaskStorageService) RemoveFromTaskTable(tid int64) bool {
	service.connection.Do("HDEL", service.taskTable, tid)
	return true
}
func (service *TaskStorageService) InsertToWaitingQ(t string) bool {
	//只需要把taskid放到 waitingQ中
	taskId, ok := service.getTaskID(t)
	if !ok {
		return false
	}
	service.connection.Do("ZADD", service.waitingQ, taskId, t)
	return true
}
func (service *TaskStorageService) AppendToTaskTable(t string) bool {
	taskId, ok := service.getTaskID(t)
	if !ok {
		return false
	}
	fmt.Println("AppendToTaskTable")
	// 插入到task table中h
	reply, err:= service.connection.Do("HSET", service.taskTable, taskId, t)
	fmt.Println("reply is",reply)
	fmt.Println("error is",err)
	return true
}

func (service *TaskStorageService) deserialize(t string) (map[string]interface{}, error) {
	taskMap := make(map[string]interface{})
	fmt.Println(t)
	if err := json.Unmarshal([]byte(t), &taskMap); err != nil {
		//todo log error
		fmt.Println("task 输入错误")
		return nil, errors.New("task 输入错误")
	}
	return taskMap, nil
}

func (service *TaskStorageService) getTaskID(t string) (taskid int64, ok bool) {
	taskMap, err := service.deserialize(t)
	if err != nil {
		return 0, false
	}
	_taskId, found := taskMap[strings.ToLower("ID")]
	if !found {
		//todo log error
		fmt.Println("not found id")
		return 0, false
	}
	var _err error
	taskid ,_err =  strconv.ParseInt(_taskId.(string),10,64)
	if _err != nil {
		fmt.Println("conv error")
	}
	return taskid, true
}
func (service *TaskStorageService) getTaskToRunAt(t string) (int64, bool) {
	taskMap, err := service.deserialize(t)
	if err != nil {
		return 0, false
	}
	taskToRun, found := taskMap[strings.ToLower("ToRunAt")]
	if !found {
		//todo log error
		fmt.Println("ToRunAt not found")
		return 0, false
	}
	toRunAt := taskToRun.(int64)
	return toRunAt, true
}

func NewTaskStorageService(ctx context.Context, url string, keepalive time.Duration, topic string,
	namePrefix string, taskComing TaskComingFunc) StorageService {

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
		keepalive:          keepalive,
		topic:              topic,
		waitingQ:           namePrefix + ":waitingSortedSet", //保存 ToRunAt taskid
		ongoingQ:           namePrefix + ":ongoingSet",       // 保存 taskid
		taskTable:          namePrefix + ":allTasksHash",     //保存taskid + serialized task
		remoteTaskCallback: taskComing,
		quitChan:           make(chan bool, 1),
	}
	// 当前连接已经订阅 topic 因此需要
	t.subCon.Subscribe(t.topic)
	return t
}