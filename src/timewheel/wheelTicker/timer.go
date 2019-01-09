/*
时间轮定时器，启动一个空的
*/

package wheelTicker

import (
	"time"
	"timewheel/logger"
	"timewheel/observer"
)

//var MinInterval time.Duration

const (
	Ready = iota
	Running
)

func init() {
	//MinInterval, _ = time.ParseDuration("100ms")
}

type WheelTicker interface {
	observer.Notifier
	Start()
	Stop()
	IsRunning() bool
	Count() uint64
}

type WheelTimer struct {
	interval  time.Duration
	ticker    *time.Ticker
	running   uint8
	count     uint64
	doneChan  chan bool
	listeners map[observer.Listener]bool
}

//type WheelTimer struct {
//
//}

func (t *WheelTimer) Register(listener observer.Listener) bool {

	if found := t.listeners[listener]; !found {
		t.listeners[listener] = true
		return true
	}
	return false
}
func (t *WheelTimer) Unregister(listener observer.Listener) bool {
	if found := t.listeners[listener]; found {
		delete(t.listeners, listener)
		return true
	}
	return false
}
func (t *WheelTimer) Trigger() {
	// 计算触发的次数, 这里只会有一个go routine执行，可以不需要锁
	t.count += 1
	for k, v := range t.listeners {
		if v {
			k.EventOccur()
		}
	}
}

func (t *WheelTimer) Start() {
	//t.ticker = time.NewTicker(time.Millisecond * 100)
	t.ticker = time.NewTicker(t.interval)
	t.running = 1

	// 启动另外一个goroutine发送事件
	go func() {
		for {
			select {
			// 先处理结束，不再发送
			case <-t.doneChan:
				return
			case <-t.ticker.C:
				// 发送通知
				logger.Logger.Infof("\n**** send timer event**********************************\n")
				t.Trigger()
			}
		}
	}()
}

// 如何安全的退出这个timer,注册一个signal事件，收到事件之后在回调中 stop ticker并关闭channel
func (t *WheelTimer) Stop() {
	t.doneChan <- true
	t.ticker.Stop()
	close(t.doneChan)
}
func (t *WheelTimer) IsRunning() bool {
	return t.running == Running
}
func (t *WheelTimer) Count() uint64 {
	return t.count
}

func NewWheelTicker(interval time.Duration) WheelTicker {
	t := &WheelTimer{interval: interval, ticker: nil, running: 0, count: 0,
		doneChan:  make(chan bool, 1),
		listeners: make(map[observer.Listener]bool)}
	t.running = Ready

	return t
}
