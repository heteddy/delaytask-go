package tracker

import "sync"

type EventHandler interface {
	HandleEvent(event Event)
}

const (
	TASK_COMPLETE = iota
	TASK_Add
	TASK_RECEIVED
	TASK_LOADING
	TASK_LOAD_ONGOING
)

type Event interface {
	GetType() int
	GetTaskID() string
	GetTask() string
}
type TaskLoadOngoing struct {
}
func (e *TaskLoadOngoing) GetType() int {
	return TASK_LOAD_ONGOING
}
func (e *TaskLoadOngoing) GetTaskID() string {
	return ""
}
func (e *TaskLoadOngoing) GetTask() string {
	return ""
}
type TaskLoadingEvent struct {
}
func (e *TaskLoadingEvent) GetType() int {
	return TASK_LOADING
}
func (e *TaskLoadingEvent) GetTaskID() string {
	return ""
}
func (e *TaskLoadingEvent) GetTask() string {
	return ""
}

type TaskCompleteEvent struct {
	TaskId string
}

func (e *TaskCompleteEvent) GetType() int {
	return TASK_COMPLETE
}
func (e *TaskCompleteEvent) GetTaskID() string {
	return e.TaskId
}
func (e *TaskCompleteEvent) GetTask() string {
	return e.TaskId
}

type TaskReceivedEvent struct {
	Task string
}
func (e *TaskReceivedEvent) GetType() int {
	return TASK_RECEIVED
}
func (e *TaskReceivedEvent) GetTaskID() string {
	return ""
}
func (e *TaskReceivedEvent) GetTask() string {
	return e.Task
}

type TaskAddEvent struct {
	Task string
}

func (e *TaskAddEvent) GetType() int {
	return TASK_Add
}
func (e *TaskAddEvent) GetTaskID() string {
	return ""
}
func (e *TaskAddEvent) GetTask() string {
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

func (t *TaskTracker) Subscribe(taskType int, l EventHandler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.listeners[taskType] = l
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