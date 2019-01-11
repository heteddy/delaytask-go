package timeService

import (
	"fmt"
	"testing"
)

type MyListener struct {
	id    int
	count int64
}

func (m *MyListener) EventOccur() {
	m.count++
	fmt.Println("event occured", m.id, m.count)
}

func TestNewWheelTicker(t *testing.T) {
	//ticker := NewWheelTicker(time.Second * 1)
	//ticker.Register(&MyListener{1, 0})
	//ticker.Register(&MyListener{2, 0})
	//ticker.Start()
	//time.Sleep(time.Second * 30)
	//ticker.Stop()
	t.Log("hello world")
}
