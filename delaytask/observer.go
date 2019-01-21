package delaytask

type Listener interface {
	EventOccur()
}
type Notifier interface {
	Trigger()
	Register(listener Listener) bool
	Unregister(listener Listener) bool
}
