package wheel

import (
	"github.com/sirupsen/logrus"
	"time"
	"wheelLogger"
)

//import (
//	"timewheel/wheel"
//)

type IWorker interface {
	Execute(Runner)
}

type Worker struct {
	taskChan chan Runner
	quitChan chan bool
	wheeler  Wheeler
}

func NewWorker(taskChan chan Runner, wheeler Wheeler) *Worker {
	w := &Worker{
		taskChan: taskChan,
		quitChan: make(chan bool, 1),
		wheeler:  wheeler,
	}
	return w
}

func (w *Worker) runTask(t Runner) {
	if t != nil {
		duration := t.GetTimeout()
		// 如果设置timeout，启动一个新的goroutine
		start := time.Now()
		if duration > 0 {

			errorChan := make(chan error, 1)
			go func(t Runner, w *Worker) {
				_, err := t.Run()
				if err != nil {
					t.SetError(err)
				}
				errorChan <- err
			}(t, w)

			select {
			case <-errorChan:
				t.Next(w.wheeler)
				// 如果发生错误，是否还继续添加到时间轮
			case <-time.After(duration):
				// 打印任务结束当前的任务
				// 运行超时不再添加
				wheelLogger.Logger.WithFields(logrus.Fields{
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
			t.Next(w.wheeler)
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
	wheeler  Wheeler
}

func NewPool(count int, taskChan chan Runner, wheeler Wheeler) *Pool {
	p := &Pool{
		count:    count,
		taskChan: taskChan,
		wheeler:  wheeler,
	}
	p.pool = make([]*Worker, count)
	for i := 0; i < count; i++ {
		p.pool[i] = NewWorker(taskChan, p.wheeler)
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
