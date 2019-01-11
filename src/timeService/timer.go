/*
时间轮定时器，启动一个空的
*/

package timeService

import (
	"time"
	"observer"
	"sync"
	"fmt"
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

func (f *WheelTimingService) Start() {
	f.ticker = time.NewTicker(f.baseTimer)
	f.wg.Add(1)
	// 启动另外一个goroutine发送事件
	go func() {
	loop:
		for {
			select {
			// 先处理结束，不再发送
			case <-f.doneChan:
				break loop

			case <-f.ticker.C:
				// 发送通知
				// 这里上锁？
				f.mu.RLock()
				// todo 这里警告
				// 给每个timer发送定时器到时
				for _, v := range f.timerMap {
					v.Tick()
				}
				f.mu.RUnlock()
			}
		}
		wheelLogger.Logger.WithFields(logrus.Fields{
			"time": time.Now(),
		}).Infoln("exit timer factory!!")
		f.wg.Done()
	}()
}
func (f *WheelTimingService) Stop() {
	f.doneChan <- true
	close(f.doneChan)
	f.ticker.Stop()
	f.wg.Wait()
}
func (f *WheelTimingService) GetTimer(duration string) AbstractTimer {
	d, err := time.ParseDuration(duration)

	if err != nil {
		//
		fmt.Println("给定的duration string错误")
		panic("给定的duration string错误")
	}
	if d.Nanoseconds() < f.baseTimer.Nanoseconds() {
		fmt.Println("给定的duration string错误")
		panic("不能小于base精度")
	}
	if d.Nanoseconds()%f.baseTimer.Nanoseconds() != 0 {
		fmt.Println("timer的精度必须是base timer的整数倍", d.Nanoseconds(), f.baseTimer.Nanoseconds())
		panic("timer的精度必须是base timer的整数倍")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if timer, found := f.timerMap[duration]; found {
		return timer
	} else {
		// create a timer and insert into timerMap
		t := &WTimer{
			interval:    d,
			description: duration,
			tickerCount: d.Nanoseconds() / f.baseTimer.Nanoseconds(),
			listeners:   make(map[observer.Listener]bool),
			mu:          sync.RWMutex{},
			index:       0,
		}

		f.timerMap[duration] = t
		return t
	}
}
