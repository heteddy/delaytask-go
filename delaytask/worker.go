package delaytask

import (
	"github.com/sirupsen/logrus"
	"time"
	"strconv"
	"runtime/debug"
)

type Worker struct {
	taskChan chan Runner
	quitChan chan bool
}

func NewWorker(taskChan chan Runner) *Worker {
	w := &Worker{
		taskChan: taskChan,
		quitChan: make(chan bool, 1),
	}
	return w
}

func (w *Worker) notifyComplete(t Runner) {
	taskID := strconv.FormatInt(t.GetID(), 10)
	Tracker.Publish(
		&TaskCompleteEvent{
			TaskId: taskID,
		})
}
func (w *Worker) addPeriodTask(t Runner) {
	if t.GetType() == PeriodTask {
		if !t.IsTaskEnd() {
			t.UpdateToRunAt()
			s := t.ToJson()
			if len(s) > 0 {
				Tracker.Publish(
					&TaskAddEvent{
						Task: s,
					})
			}

		} else {
			Logger.WithFields(logrus.Fields{
				"type": t.GetType(),
				"id":   t.GetID(),
			}).Infoln("addPeriodTask task end!!")
		}
	}
}

func (w *Worker) runTask(t Runner) {
	if t != nil {
		duration := t.GetTimeout()
		// 如果设置timeout，启动一个新的goroutine
		start := time.Now()
		if duration > 0 {
			finishChan := make(chan bool, 1)
			unexpectedError := make(chan interface{}, 1)
			go func(t Runner, w *Worker) {
				defer func() {
					if err := recover(); err != nil {
						Logger.WithFields(logrus.Fields{
							"err": err,
						}).Errorln("run task unexpected panic，" +
							"will not run this task again, although it is period task")
						debug.PrintStack()
						unexpectedError <- err
					}
				}()

				_, err := t.Run()
				if err != nil {
					Logger.WithFields(logrus.Fields{
						"err": err,
					}).Warnln("run task got an expected error")
					t.SetError(err)

				}
				finishChan <- true

			}(t, w)

			select {
			case <-unexpectedError:
				// 发生了未捕获的异常，就不再执行这个task了
				w.notifyComplete(t)
			case <-finishChan:
				w.notifyComplete(t)
				w.addPeriodTask(t)
			case <-time.After(duration):
				// 打印任务结束当前的任务
				// 运行超时不再添加
				w.notifyComplete(t)
				w.addPeriodTask(t)
				Logger.WithFields(logrus.Fields{
					"start-time":   start.Format("2006-01-02 15:04:05.999999"),
					"now":          time.Now().Format("2006-01-02 15:04:05.999999"),
					"taskID":       t.GetID(),
					"task.Timeout": t.GetTimeout(),
				}).Errorln("error.......timeout")
			}

		} else {
			_, err := t.Run()
			if err != nil {
				t.SetError(err)
			}
			// 具体的task实现这个轮
			//先加入在运行
			w.notifyComplete(t)
			w.addPeriodTask(t)
		}
	}
}

func (w *Worker) Start() {
	go func() {
		//
		for {
			select {
			// 如果有任务可以执行，就不停止？停止的时候先关闭ticker，因此不会持续的写入；执行完queue的任务就停止
			case t := <-w.taskChan:
				w.runTask(t)
			case <-w.quitChan:
				break
			default:
				// 实现优先级的方式，必须把前面的都写一遍，不然就会阻塞到这里不执行任务
				select {
				case t := <-w.taskChan:
					w.runTask(t)
				case <-w.quitChan:
					break
				}
			}
		}

	}()
}
func (w *Worker) Stop() {
	w.quitChan <- true
}

type Pool struct {
	count    int
	pool     []*Worker
	taskChan chan Runner
}

func NewPool(count int, taskChan chan Runner) *Pool {
	p := &Pool{
		count:    count,
		taskChan: taskChan,
	}
	p.pool = make([]*Worker, count)
	for i := 0; i < count; i++ {
		p.pool[i] = NewWorker(taskChan)
	}
	return p
}

func (p *Pool) Start() {
	for _, w := range p.pool {
		w.Start()
	}
}

func (p *Pool) Stop() {

	for _, w := range p.pool {
		w.Stop()
	}
}

func (p *Pool) Execute(runner Runner) {
	p.taskChan <- runner
}
