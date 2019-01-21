package trace

import (
	"strconv"
	"sync"
	"time"
)

var STARTTIME time.Time

func init() {
	start, _ := time.Parse(time.RFC3339, "2018-01-01T00:00:00+08:00")
	STARTTIME = start
}

type TracingID int64
type RequestTrace interface {
	GetID() TracingID
}

var MaxSequenceID = 0xFFF
var HostID = 210

type traceID struct {
	mu             sync.Mutex
	sequenceID     int16
	hostID         uint32
	startTimestamp int64
}

func (trace TracingID) Time() time.Time {

	timeStamp := int64(trace) & 0x7FFFFFFFFFC00000 >> 22
	logTimeStamp := timeStamp + STARTTIME.UnixNano()/1e6
	seconds := logTimeStamp / 1000
	milliSeconds := logTimeStamp - seconds*1000
	return time.Unix(seconds, milliSeconds*1e6)
}
func (trace TracingID) Host() int32 {
	return int32((int64(trace) >> 12) & 0x3FF)
}
func (trace TracingID) Sequence() int16 {
	return int16(int64(trace) & 0xFFF)
}
func (trace TracingID) Int64() int64 {
	return int64(trace)
}
func (trace TracingID) String() string {
	return strconv.FormatInt(int64(trace), 10)
}

func NewTrace(hostID uint32) RequestTrace {
	t := &traceID{
		sync.Mutex{},
		0,
		hostID, STARTTIME.UnixNano() / 1e6,
	}
	return t
}

func (trace *traceID) GetID() TracingID {
	trace.mu.Lock()
	defer trace.mu.Unlock()

	return TracingID(trace.generateTimestamp() | trace.generateHostID() | trace.generateSequenceID())
}
func (trace *traceID) setHostID(host uint32) (bool, error) {
	trace.hostID = host
	return true, nil
}
func (trace *traceID) setStartTime() {
	// 强制从2018年开始
	trace.startTimestamp = STARTTIME.UnixNano() / 1e6
}
func (trace *traceID) generateSequenceID() uint64 {
	if int64(trace.sequenceID) >= int64(MaxSequenceID) {
		trace.sequenceID = 0
	} else {
		trace.sequenceID += 1
	}

	return BitOp(uint64(trace.sequenceID), 64, 0xFFF)
}
func (trace *traceID) generateHostID() uint64 {
	hostID := trace.hostID << 12
	hid := BitOp(uint64(hostID), 64, 0x3FF000)
	return hid
}
func (trace *traceID) generateTimestamp() uint64 {
	now := time.Now().UnixNano() / 1e6
	delta := now - trace.startTimestamp

	myTime := delta << 22
	ts := BitOp(uint64(myTime), 64, 0x7FFFFFFFFFC00000)
	return ts
}

func BitOp(input uint64, bitCount uint8, mask uint64) (ret uint64) {
	ret = input

	switch bitCount {
	case 8:
		ret &= 0xFF
	case 16:
		ret &= 0xFFFF
	case 32:
		ret &= 0xFFFFFFFF
	case 64:
		ret &= 0xFFFFFFFFFFFFFFFF
	}
	ret &= mask
	return
}
