package wheel

import (
	"container/list"
	"github.com/sirupsen/logrus"
	"math"
	"time"
	"timewheel/logger"
	"timewheel/wheelTicker"
)

func round(input float64) int64 {
	return int64(math.Floor(input + 0.5))
}

/*
TimeWheel的接口
*/
type Wheeler interface {
	Add(Runner)
	// 通过runnerID 删除一个Runner
	Remove(int64)
	//RemoveByRunnerID(int64)
	//ExecuteCommand(command Command)
	ListRunner()
}

/*
包含一个Runner，在wheel中需要round 轮；
执行task的worker
*/
type TaskNode struct {
	Runner
	round  uint64  // 当运行轮数之后，再运行
	worker IWorker // 执行task的worker pool
}

// todo 这里单独运行一个go routine不然要死锁，因为这里可能导致再次加入timeWheel
//
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
	wheeler   Wheeler
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
	logger.Logger.WithFields(logrus.Fields{
		"node-length": n.taskNodes.Len(),
	}).Infoln("go through task node")
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
			logger.Logger.WithFields(logrus.Fields{}).Errorln("error convert task")
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
	index int64
	slots []*Node // 时间轮插槽，每个包含TaskNode 链表和taskNode count
	//pUpstream   *Wheel
	//pDownstream *Wheel
	// 这里所有的TimeWheel使用一个全局的读写锁，防止遍历的时候导致错误
	//mu *sync.RWMutex
	wheeler   Wheeler
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
	logger.Logger.WithFields(logrus.Fields{
		"now": time.Now().Format("2006-01-02 15:04:05.999999"),
	}).Infoln("ticks")
	w.index = (w.index + 1) % w.count
	w.slots[w.index].walk()
}

func (w *Wheel) GetTaskInfo() string {
	for k, v := range w.runnerMap {
		logger.Logger.WithFields(logrus.Fields{
			"taskID":     k,
			"toRunAfter": v.task.GetToRunAfter(),
		}).Infoln("GetTaskInfo")
	}
	return ""
}

/*
首先根据执行时间计算放入那个slot，然后保存到RunnerMap中，方便查询
*/
func (w *Wheel) AddRunner(runner Runner, pool *Pool) {

	toRun := runner.GetToRunAfter()
	index := w.index
	switch {
	case toRun < w.ticks:
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
		logger.Logger.WithFields(logrus.Fields{
			"insert-index": idx,
			"currentIdx":   index,
			"round":        taskNode.round,
			"toRunAfter":   toRun,
			"taskID":       taskNode.GetID(),
		}).Infoln("delta < w.ring.ticks")
	case toRun > w.ticks:
		ticks := int64(w.ticks)
		roundDuration := int64(w.count) * ticks

		// 先计算需要多少轮
		rounds := int64(toRun) / roundDuration
		// 然后是
		delay := round(float64(int64(toRun)%roundDuration) / float64(ticks))

		taskNode := &TaskNode{runner,
			uint64(rounds),
			pool,
		}
		idx := int64(int64(index)+delay) % int64(w.count)

		w.slots[idx].insert(taskNode)
		w.runnerMap[runner.GetID()] = &runnerInfo{
			taskNode,
			idx,
		}
		logger.Logger.WithFields(logrus.Fields{
			"insert-index": idx,
			"currentIdx":   index,
			"round":        taskNode.round,
			"toRunAfter":   toRun,
			"taskID":       taskNode.GetID(),
		}).Infoln("delta > w.ring.ticks")

	default:
		logger.Logger.WithFields(logrus.Fields{}).Infoln("not added?")
	}
}

/*
删除操作发生在time wheel线程中，锁保护runnerMap
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

type Command interface {
	Execute(w *TimeWheeler)
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

type listRunnersCommand struct {
}

func (cmd *listRunnersCommand) Execute(w *TimeWheeler) {
	w.actualListRunner()
}

type TimeWheeler struct {
	ring     *Wheel
	ticker   wheelTicker.WheelTicker
	pool     *Pool
	cmdChan  chan Command
	quitChan chan bool
}

func NewTimeWheel(duration string, slot int, worker int) *TimeWheeler {
	tickerDuration, err := time.ParseDuration(duration)
	if err != nil {
		panic(err)
		return nil
	}
	logger.Logger.WithFields(logrus.Fields{
		duration: tickerDuration,
	})
	// 创建一个timer
	timeWheeler := &TimeWheeler{
		nil,
		wheelTicker.NewWheelTicker(tickerDuration),
		nil,
		make(chan Command, worker), //带缓存
		make(chan bool, 1),
	}
	// task channel 带缓冲，防阻塞Time wheeler的运行
	taskChan := make(chan Runner, worker*2)
	pool := NewPool(worker, taskChan, timeWheeler)
	timeWheeler.pool = pool

	slots := make([]*Node, slot, slot)
	for i := 0; i < len(slots); i++ {
		slots[i] = &Node{
			taskNodes: list.List{},
			wheeler:   timeWheeler,
		}
	}
	wheel := &Wheel{
		ticks: tickerDuration,
		// 时间轮 槽的数量
		count:     int64(slot),
		index:     -1,
		slots:     slots,
		runnerMap: make(map[int64]*runnerInfo),
		wheeler:   timeWheeler,
	}
	timeWheeler.ring = wheel

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
		logger.Logger.WithFields(logrus.Fields{}).Errorln("TimeWheeler exit error")
	}()
	wheel.ticker.Start()
	wheel.pool.Start()
}
func (wheel *TimeWheeler) Stop() {
	wheel.quitChan <- true
	wheel.ticker.Stop()
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
func (wheel *TimeWheeler) ListRunner() {
	wheel.cmdChan <- &listRunnersCommand{}
}
func (wheel *TimeWheeler) actualListRunner() {
	wheel.ring.GetTaskInfo()
}

func (wheel *TimeWheeler) String() string {
	return ""
}
func (wheel *TimeWheeler) EventOccur() {
	wheel.cmdChan <- &tickCommand{}
}
func (wheel *TimeWheeler) actualTick() {

	wheel.ring.Tick()
}
