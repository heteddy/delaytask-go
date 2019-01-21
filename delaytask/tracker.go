package delaytask

import "sync"

type EventHandler interface {
	HandleEvent(event Event)
}

const (
	TaskCompleteEventType        = iota
	TaskAddEventType
	TaskReceivedEventType
	PeriodTaskLoadingEventType
	TaskLoadingOngoingsEventType
)

type Event interface {
	GetType() int
	GetBody() string
}
type TaskLoadOngoing struct {
}

func (e *TaskLoadOngoing) GetType() int {
	return TaskLoadingOngoingsEventType
}
func (e *TaskLoadOngoing) GetBody() string {
	return ""
}

type TaskLoadingEvent struct {
}

func (e *TaskLoadingEvent) GetType() int {
	return PeriodTaskLoadingEventType
}
func (e *TaskLoadingEvent) GetBody() string {
	return ""
}

type TaskCompleteEvent struct {
	TaskId string
}

func (e *TaskCompleteEvent) GetType() int {
	return TaskCompleteEventType
}
func (e *TaskCompleteEvent) GetBody() string {
	return e.TaskId
}

type TaskReceivedEvent struct {
	Task string
}

func (e *TaskReceivedEvent) GetType() int {
	return TaskReceivedEventType
}

func (e *TaskReceivedEvent) GetBody() string {
	return e.Task
}

type TaskAddEvent struct {
	Task string
}

func (e *TaskAddEvent) GetType() int {
	return TaskAddEventType
}
func (e *TaskAddEvent) GetBody() string {
	return e.Task
}

type TaskTracker struct {
	mu        sync.RWMutex
	listeners map[int]EventHandler
}

var Tracker *TaskTracker

func init() {
	Tracker = &TaskTracker{
		mu:        sync.RWMutex{},
		listeners: make(map[int]EventHandler),
	}
}

func (t *TaskTracker) Subscribe(taskType int, handler EventHandler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.listeners[taskType] = handler
}

func (t *TaskTracker) Publish(event Event) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for k, v := range t.listeners {
		if event.GetType() == k {
			v.HandleEvent(event)
		}
	}
}
