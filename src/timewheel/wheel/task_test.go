package wheel

import (
	"testing"
	"time"
	"encoding/json"
	"fmt"
)

func TestJsonDuration(t *testing.T) {
	d := TaskDuration(time.Minute)
	data, _:= json.Marshal(d)
	fmt.Println(string(data))
	dataStr := string(data)

	var duration TaskDuration
	err:=json.Unmarshal([]byte(dataStr),&duration)
	fmt.Println(err,duration)
	t.Log("ok")
}

func TestJsonTime(t *testing.T) {
	d := TaskTime(time.Now())
	data, _:= json.Marshal(d)
	fmt.Println(string(data))
	dataStr := string(data)

	var taskTime TaskTime
	err:=json.Unmarshal([]byte(dataStr),&taskTime)
	fmt.Println(err,time.Time(taskTime))
	t.Log("ok")
}