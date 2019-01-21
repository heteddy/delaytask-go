package wheel

import (
	"container/list"
	"github.com/sirupsen/logrus"
	"math"
	"time"
	"wheelLogger"
	"timewheel/timeService"
	"runtime"
)

func roundFloatToInt64(input float64) int64 {
	return int64(math.Floor(input + 0.5))
}

/*
包含一个Runner，在wheel中需要round 轮；
执行task的worker
*/
type TaskNode struct {
	Runner
	round uint64 // 当运行轮数之后，再运行
	// pool 的接口
	worker IWorker // 执行task的worker pool
}

func (t *TaskNode) runNode() (run bool) {

	if t.round == 0 {
		t.worker.Execute(t.Runner)
		run = true
	} else {
		t.round--
		run = false
	}
	return
}

// Node 中元素为TaskNode
type Node struct {
	// taskNodes 元素为*TaskNode类型
	taskNodes list.List //时间轮每个Node包含一个task 链表
}

func (n *Node) count() int {
	return n.taskNodes.Len()
}

func (n *Node) insert(t *TaskNode) {
	n.taskNodes.PushBack(t)
}
func (n *Node) remove(t *TaskNode) {
	for node := n.taskNodes.Front(); node != nil; {
		if task, ok := node.Value.(*TaskNode); ok {
			if task == t {
				next := node.Next()
				n.taskNodes.Remove(node)
				node = next
				return
			} else {
				node = node.Next()
			}
		} else {
			node = node.Next()
		}
	}
}

// 遍历当前的list
func (n *Node) walk() {
	for node := n.taskNodes.Front(); node != nil; {
		// 执行
		if task, ok := node.Value.(*TaskNode); ok {
			// 这里在新的goroutine中运行
			// 运行了之后就删除这个节点，如果还需要运行，会在runner的next方法中执行
			if run := task.runNode(); run {
				next := node.Next()
				// 删除当前的task Node,通过Wheeler删除
				// 这里可以直接删除，因为当前运行就是
				n.taskNodes.Remove(node)
				node = next
			} else { // 别忘了这个条件
				node = node.Next()
			}
		} else {
			wheelLogger.Logger.WithFields(logrus.Fields{}).Errorln("error convert task")
			node = node.Next()
		}
		// 下一个

	}
}

/*
用于追踪，查找，监控task的运行状态等
*/
type runnerInfo struct {
	task *TaskNode // runner所在的taskNode
	slot int64     // runner在wheel中的插槽
}

/*
包含当前运行过的或者正在运行的slot 索引，
这里初始化之后就不会再增加新的slot，因此不需要使用锁了，由于timer也是在一个go routine运行，因此所有的修改操作不需要任何锁操作；
*/
type Wheel struct {
	ticks time.Duration
	// 时间轮 槽的数量
	count int64
	// 索引
	index int64
	// 槽
	slots []*Node // 时间轮插槽，每个包含TaskNode 链表和taskNode count
	// 保存task和task所在的index
	runnerMap map[int64]*runnerInfo
}

// 获取槽位代表的时间间隔
func (w *Wheel) SlotTicks() time.Duration {
	return w.ticks
}

// 返回当前的轮 运行的槽位
func (w *Wheel) Index() int64 {
	return w.index
}

// 获取task的个数
func (w *Wheel) TaskCount() int64 {
	var total int64
	// 计算所有的task 总和
	for _, t := range w.slots {
		total += int64(t.count())
	}
	return total
}

func (w *Wheel) Tick() {
	w.index = (w.index + 1) % w.count
	w.slots[w.index].walk()
}

func (w *Wheel) GetTaskInfo() string {
	for k, v := range w.runnerMap {
		wheelLogger.Logger.WithFields(logrus.Fields{
			"taskID":  k,
			"toRunAt": v.task.GetToRunAt(),
		}).Infoln("getTaskInfo")
	}
	return ""
}
func (w *Wheel) RoundTicks() time.Duration {
	return time.Duration(int64(w.ticks) * w.count)
}

/*
首先根据执行时间计算放入那个slot，然后保存到RunnerMap中，方便查询
*/
func (w *Wheel) AddRunner(runner Runner, pool *Pool) {
	toRunAt := runner.GetToRunAt()

	delta := toRunAt.Sub(time.Now()) // 需要进行round计算
	// 已经超时时间轮的一半
	if delta.Seconds() < 0 && int64(math.Abs(delta.Seconds())) > int64(w.RoundTicks())/2 {
		wheelLogger.Logger.WithFields(logrus.Fields{
			"toRunAt": toRunAt,
			"delta":   delta,
			"now":     time.Now(),
		}).Errorln("AddRunner")
		return
	}
	//
	deltaSeconds := roundFloatToInt64(delta.Seconds())
	tickSeconds := roundFloatToInt64(w.ticks.Seconds())
	roundDurationSeconds := tickSeconds * w.count

	index := w.index
	switch {
	case deltaSeconds < tickSeconds: // 执行时间 < 一个slot时间，直接插入下一个slot
		taskNode := &TaskNode{runner,
			0,
			pool,
		}
		idx := (index + 1) % w.count
		w.slots[idx].insert(taskNode)
		w.runnerMap[runner.GetID()] = &runnerInfo{
			taskNode,
			idx,
		}
	case deltaSeconds > tickSeconds:
		// 先计算需要多少轮
		rounds := deltaSeconds / roundDurationSeconds

		secondsLeft := deltaSeconds - rounds*roundDurationSeconds
		// 2. 计算需要的偏移
		offset := secondsLeft / tickSeconds
		if secondsLeft%tickSeconds != 0 {
			offset += 1
		}
		var idx int64
		//index != -1, 说明开始转动，正常逻辑就是直接加上当前的偏移
		idx = int64(int64(index)+offset) % int64(w.count)

		// 临界条件，如果恰好是整数圈，调整rounds数
		// 由于添加任务操作和timer 触发运行task在一个goroutine中，因此可以保证顺序性
		// 也就是这里的index里面的task已经被执行过了
		if offset == 0 {
			if rounds > 0 {
				rounds -= 1
			} else {
				wheelLogger.Logger.WithFields(logrus.Fields{
					"insert-index":      idx,
					"currentIdx":        index,
					"roundFloatToInt64": rounds,
					"offset":            offset,
					"delta":             delta,
					"taskID":            runner.GetID(),
				}).Errorln("offset == 0 and rounds == 0!!!")
			}
		}
		taskNode := &TaskNode{runner,
			uint64(rounds),
			pool,
		}
		w.slots[idx].insert(taskNode)
		w.runnerMap[runner.GetID()] = &runnerInfo{
			taskNode,
			idx,
		}
		//wheelLogger.Logger.WithFields(logrus.Fields{
		//	"insert-index":      idx,
		//	"currentIdx":        index,
		//	"roundFloatToInt64": taskNode.round,
		//	"offset":            offset,
		//	"delta":             delta,
		//	"taskID":            taskNode.GetID(),
		//}).Infoln("delta > w.ring.ticks")

	default:
		wheelLogger.Logger.WithFields(logrus.Fields{}).Infoln("not added?")
	}
}

/*
删除操作发生在time wheel线程中
*/
func (w *Wheel) RemoveRunner(runnerID int64) {
	if info, ok := w.runnerMap[runnerID]; !ok {
		return
	} else {
		slot := info.slot
		if slot < w.count {
			w.slots[slot].remove(info.task)
			delete(w.runnerMap, runnerID)
		}
	}

}

type addRunnerCommand struct {
	r Runner
}

func (cmd *addRunnerCommand) Execute(w *TimeWheeler) {
	w.actualAdd(cmd.r)
}

type removeRunnerCommand struct {
	runnerID int64
}

func (cmd *removeRunnerCommand) Execute(w *TimeWheeler) {
	w.actualRemove(cmd.runnerID)
}

type tickCommand struct {
}
func (cmd *tickCommand) Execute(w *TimeWheeler) {
	w.actualTick()
}
type Command interface {
	Execute(w *TimeWheeler)
}
type TimeWheeler struct {
	ring     *Wheel
	ticker   timeService.AbstractTimer
	pool     *Pool
	cmdChan  chan Command
	quitChan chan bool
}

func NewTimeWheel(duration string, slot int) *TimeWheeler {
	worker := runtime.NumCPU() * 5
	tickerDuration, err := time.ParseDuration(duration)
	if tickerDuration.Seconds() < 1 {
		wheelLogger.Logger.WithFields(logrus.Fields{
			"duration": duration,
		}).Fatalln("目前只支持tick为1s及其以上的任务")
	}
	if err != nil {
		panic(err)
		return nil
	}

	slots := make([]*Node, slot, slot)
	for i := 0; i < len(slots); i++ {
		slots[i] = &Node{
			taskNodes: list.List{},
		}
	}
	wheel := &Wheel{
		ticks: tickerDuration,
		// 时间轮 槽的数量
		count:     int64(slot),
		index:     0,
		slots:     slots,
		runnerMap: make(map[int64]*runnerInfo),
	}
	// task channel 带缓冲，防阻塞Time wheeler的运行
	taskChan := make(chan Runner, worker*2)
	pool := NewPool(worker, taskChan)
	factory := timeService.TimerService

	// 创建一个timer
	timeWheeler := &TimeWheeler{
		wheel,
		factory.GetTimer(duration),
		pool,
		make(chan Command, worker), //带缓存
		make(chan bool, 1),
	}
	timeWheeler.ticker.Register(timeWheeler)
	return timeWheeler
}
func (wheel *TimeWheeler) Start() {
	// 先启动worker
	// 所有的操作都封装成cmd，都在当前的线程执行,因此可以不再使用锁,也可以保证线性一致性
	go func() {
		var count = 0
		for {
			select {
			case cmd := <-wheel.cmdChan:
				count++
				cmd.Execute(wheel)
			case <-wheel.quitChan:
				count++
				return
			}
		}
		wheelLogger.Logger.WithFields(logrus.Fields{}).Errorln("TimeWheeler exit error")
	}()
	wheel.pool.Start()
}
func (wheel *TimeWheeler) Stop() {
	wheel.quitChan <- true
	wheel.pool.Stop()

	time.Sleep(time.Second * 1)
	close(wheel.quitChan)
	close(wheel.cmdChan)
}

// 增加一个task进入时间轮，需要计算应该放入哪一个slot中
// 增加可能在worker goroutine中，也可能在用户的main goroutine
func (wheel *TimeWheeler) Add(r Runner) {
	// 1. 创建一个TimeWheeNode 然后计算插入位置，并插入到指定的TimeWheelNode中；
	wheel.cmdChan <- &addRunnerCommand{r}
}
func (wheel *TimeWheeler) actualAdd(r Runner) {
	wheel.ring.AddRunner(r, wheel.pool)
}
func (wheel *TimeWheeler) Remove(runnerID int64) {
	wheel.cmdChan <- &removeRunnerCommand{runnerID: runnerID}
}
func (wheel *TimeWheeler) actualRemove(runnerID int64) {
	wheel.ring.RemoveRunner(runnerID)
}
func (wheel *TimeWheeler) EventOccur() {
	wheel.cmdChan <- &tickCommand{}
}
func (wheel *TimeWheeler) actualTick() {
	wheel.ring.Tick()
}

func (wheel *TimeWheeler) RoundDuration() time.Duration {
	return time.Duration(int64(wheel.ring.ticks) * wheel.ring.count)
}
