package timewheel

import (
	"context"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"sync"
	"time"
)

type Client interface {
	Connect() (bool, error)
	Start()
	Stop()
	Close()
}

type RedisClient struct {
	url        string
	keepalive  time.Duration
	connection redis.Conn
	quitChan   chan bool
}

func (rc *RedisClient) Connect() (bool, error) {
	var err error
	rc.connection, err = redis.DialURL(su.url, redis.DialReadTimeout(su.keepalive+10*time.Second),
		redis.DialWriteTimeout(10*time.Second))
	if err != nil {
		return false, err
	}
	return true, nil
}
func (rc *RedisClient) Close() {
	rc.connection.Close()
}

func (rc *RedisClient) Start() {
	go func() {
		ticker := time.NewTicker(rc.keepalive)
		var err error
	loop:
		for err == nil {
			select {
			case <-ticker.C:
				rc.connection.Send("PING", "")
				if err = rc.connection.Flush(); err != nil {
				}
			case <-rc.quitChan:
				break loop
			}
		}
		fmt.Println("exit startKeepalive!!")
	}()
}

func (rc *RedisClient) Stop() {
	rc.quitChan <- true
}

type Subscriber interface {
	Start()
	Stop()
}

type taskSubscriber struct {
	conn        *RedisClient
	subCon      redis.PubSubConn
	topic       string
	messageChan chan redis.Message
	quitChan    chan bool
	wg          sync.WaitGroup
	ctx         context.Context
	//cancel context.CancelFunc
}

//func (su *taskSubscriber) Connect() (bool, error) {
//	var err error
//	su.connection, err = redis.DialURL(su.url, redis.DialReadTimeout(su.keepalive+10*time.Second),
//		redis.DialWriteTimeout(10*time.Second))
//	if err != nil {
//		return false, err
//	}
//	su.subCon = redis.PubSubConn{Conn: su.connection}
//
//	if err := su.subCon.Subscribe(su.topic); err != nil {
//
//	}
//	return true, nil
//}

func (su *taskSubscriber) startReceive(wg sync.WaitGroup) {
	wg.Add(1)
	go func() {
	loop:
		for {
			data := su.subCon.Receive()
			switch n := data.(type) {
			case error:
				break loop
			case redis.Message:
				fmt.Println("message", n)
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
		wg.Done()
	}()
}

func (su *taskSubscriber) Start() {
	su.startReceive(su.wg)
}
func (su *taskSubscriber) Stop() {
	su.quitChan <- true
	su.subCon.Unsubscribe(su.topic)
	su.wg.Wait()
}

func NewRedisSubscriber(ctx context.Context, url string, keepalive time.Duration, topic string) *taskSubscriber {
	conn := &RedisClient{
		url:       url,
		keepalive: keepalive,
	}
	su := &taskSubscriber{
		conn:        conn,
		quitChan:    make(chan bool, 1),
		ctx:         ctx,
		topic:       topic,
		messageChan: make(chan redis.Message, 2),
		wg:          sync.WaitGroup{},
	}
	return su
}
