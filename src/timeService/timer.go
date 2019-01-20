/*
时间轮定时器，启动一个空的
*/

package timeService

import (
	"time"
	"observer"
	"sync"
	"wheelLogger"
	"github.com/sirupsen/logrus"
)

//var MinInterval time.Duration

const (
	Ready   = iota
	Running
)

func init() {
	//MinInterval, _ = time.ParseDuration("100ms")
}

type AbstractTimer interface {
	observer.Notifier
	Tick()
}
type TimingService interface {
	Start()
	Stop()
	GetTimer(duration string) AbstractTimer
	//GetTimerByDuration(duration time.Duration) AbstractTimer
}

type WTimer struct {
	interval    time.Duration
	description string
	// register and unregister will run in multiple goroutines
	mu          sync.RWMutex
	listeners   map[observer.Listener]bool
	tickerCount int64
	index       int64
}

func (t *WTimer) Tick() {
	t.mu.RLock()
	t.index ++

	if t.index%t.tickerCount == 0 {
		for k, v := range t.listeners {
			if v {
				k.EventOccur()
			}
		}
		t.index = 0
	}
	t.mu.RUnlock()
}

func (t *WTimer) Register(listener observer.Listener) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	// 防止重复注册
	if found := t.listeners[listener]; !found {
		t.listeners[listener] = true
		return true
	}
	return false
}
func (t *WTimer) Unregister(listener observer.Listener) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if found := t.listeners[listener]; found {
		delete(t.listeners, listener)
		return true
	}
	return false
}
func (t *WTimer) Trigger() {
	// 计算触发的次数, 这里只会有一个go routine执行，可以不需要锁
	for k, v := range t.listeners {
		if v {
			k.EventOccur()
		}
	}
}

var TimerService TimingService

type WheelTimingService struct {
	mu        sync.RWMutex
	timerMap  map[string]AbstractTimer
	baseTimer time.Duration
	doneChan  chan bool
	ticker    *time.Ticker
	wg        sync.WaitGroup
}

func init() {
	TimerService = &WheelTimingService{
		mu:       sync.RWMutex{},
		timerMap: make(map[string]AbstractTimer),
		// 100ms 的timer
		baseTimer: time.Millisecond * 100,
		doneChan:  make(chan bool, 1),
		wg:        sync.WaitGroup{},
	}
}

func (service *WheelTimingService) Start() {
	service.ticker = time.NewTicker(service.baseTimer)
	service.wg.Add(1)
	// 启动另外一个goroutine发送事件
	go func() {
	loop:
		for {
			select {
			// 先处理结束，不再发送
			case <-service.doneChan:
				break loop

			case <-service.ticker.C:
				// 发送通知
				// 这里上锁？
				service.mu.RLock()
				// todo 这里警告
				// 给每个timer发送定时器到时
				for _, v := range service.timerMap {
					v.Tick()
				}
				service.mu.RUnlock()
			}
		}
		wheelLogger.Logger.WithFields(logrus.Fields{
			"time": time.Now(),
		}).Warnln("exit timer factory!!")
		service.wg.Done()
	}()
}
func (service *WheelTimingService) Stop() {
	service.ticker.Stop()
	service.doneChan <- true

	service.wg.Wait()
	// todo 这里可能发生panic？？
	close(service.doneChan)
}

func (service *WheelTimingService) GetTimer(duration string) AbstractTimer {
	d, err := time.ParseDuration(duration)
	if err != nil {
		//
		wheelLogger.Logger.WithFields(logrus.Fields{
			"duration": duration,
		}).Fatalln("给定的duration string错误")
	}
	if d.Nanoseconds() < service.baseTimer.Nanoseconds() {
		wheelLogger.Logger.WithFields(logrus.Fields{
			"duration": duration,
		}).Fatalln("duration 精度太高,不能小于base精度")

	}
	if d.Nanoseconds()%service.baseTimer.Nanoseconds() != 0 {
		wheelLogger.Logger.WithFields(logrus.Fields{
			"duration":  duration,
			"basetimer": service.baseTimer,
		}).Fatalln("timer的精度必须是base timer的整数倍")
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if timer, found := service.timerMap[duration]; found {
		return timer
	} else {
		// create a timer and insert into timerMap
		t := &WTimer{
			interval:    d,
			description: duration,
			tickerCount: d.Nanoseconds() / service.baseTimer.Nanoseconds(),
			listeners:   make(map[observer.Listener]bool),
			mu:          sync.RWMutex{},
			index:       0,
		}

		service.timerMap[duration] = t
		return t
	}
}
