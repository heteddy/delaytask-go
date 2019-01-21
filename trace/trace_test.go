package trace

import (
	"math/rand"
	"testing"
	"time"
)

func TestTraceGen1(t *testing.T) {
	trace := NewTrace(0x222)
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				rand.Seed(time.Now().UnixNano())
				randomNum := rand.Int63n(100)
				time.Sleep(time.Millisecond * time.Duration(randomNum))
				t.Log(trace.GetID())
			}
		}()
	}
	t.Log("ok ok")
}
