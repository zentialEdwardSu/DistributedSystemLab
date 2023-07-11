package mr

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// Task object that hold metadata (store in Coordinator)
type Task struct {
	beginTime   time.Time
	MapIndex    int // map index
	ReduceIndex int
	FileName    string
	R           int
	M           int
}

// Now set current Task.beginTime with time.Now
func (t *Task) Now() {
	t.beginTime = time.Now()
}

// TimeOut whether this task is time outed
func (t *Task) TimeOut() bool {
	return time.Now().Sub(t.beginTime) > time.Second*10
}

func (t *Task) Log(prefix string) {
	fmt.Printf("%s: %+v\n\n", prefix, t)
}

func (t *Task) toTaskState(State int) TaskState {
	ts := TaskState{}
	ts.FileName = t.FileName
	ts.MapIndex = t.MapIndex
	ts.ReduceIndex = t.ReduceIndex
	ts.R = t.R
	ts.M = t.M
	switch State {
	case _map:
		ts.State = _map
	case _reduce:
		ts.State = _reduce
	}
	return ts
}

// TaskQueue for more detailed lock control
type TaskQueue struct {
	lock  sync.Mutex
	queue []Task
}

// Len get length of current TaskQueue
func (q *TaskQueue) Len() int {
	length := len(q.queue)
	return length
}

func (q *TaskQueue) Push(task Task) {
	q.lock.Lock()
	if (reflect.DeepEqual(task, Task{})) {
		q.lock.Unlock()
		return
	}
	//fmt.Printf("Here: %v+\n", task)
	// push task
	q.queue = append(q.queue, task)
	q.lock.Unlock()
}

func (q *TaskQueue) Pop() (Task, error) {
	q.lock.Lock()
	length := q.Len()
	if length == 0 {
		q.lock.Unlock()
		return Task{}, errors.New("[Coordinator] empty TaskQueue")
	}
	popItem := q.queue[length-1]
	q.queue = q.queue[:length-1]
	q.lock.Unlock()

	return popItem, nil
}

func (q *TaskQueue) PopTimeOut() []Task {
	timeoutTasks := make([]Task, 0)
	q.lock.Lock()
	//if q.Len() < 1 {
	//	return timeoutTasks
	//}
	for idx := 0; idx < q.Len(); {
		task := q.queue[idx]
		if task.TimeOut() {
			timeoutTasks = append(timeoutTasks, task)
			q.queue = append(q.queue[:idx], q.queue[idx+1:]...)
		} else {
			idx++
		}
	}
	q.lock.Unlock()
	return timeoutTasks
}

func (q *TaskQueue) RemoveTask(MapIndex int, ReduceIndex int) {
	q.lock.Lock()
	if q.Len() < 1 {
		return
	}
	for idx := 0; idx < q.Len(); {
		task := q.queue[idx]
		if task.MapIndex == MapIndex && task.ReduceIndex == ReduceIndex {
			q.queue = append(q.queue[:idx], q.queue[idx+1:]...)
		} else {
			idx++
		}
	}
	q.lock.Unlock()
}

type Coordinator struct {
	// Your definitions here.
	fileList []string

	mapIdle    TaskQueue
	mapProcess TaskQueue

	reduceIdle    TaskQueue
	reduceProcess TaskQueue

	R      int  // mount of reduce worker
	isDone bool // if all work is done
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// HandleCallTask handle the rpc request from worker and assign task to them
func (c *Coordinator) HandleCallTask(args *ExampleArgs, reply *TaskState) error {
	if c.isDone {
		return nil
	}

	toReduce, err := c.reduceIdle.Pop()
	if err == nil {
		toReduce.Now()
		// push to Process queue
		c.reduceProcess.Push(toReduce)
		*reply = toReduce.toTaskState(_reduce)
		reply.Log("[Coordinator]")
		fmt.Println("[Coordinator] Assign reduce work to reduce worker")
		return nil
	}

	toMap, err := c.mapIdle.Pop()
	if err == nil {
		toMap.Now()
		c.mapProcess.Push(toMap)
		*reply = toMap.toTaskState(_map)
		reply.Log("[Coordinator]")
		fmt.Println("[Coordinator] Assign map work to map worker")
		return nil
	}
	// make sure only map/reduce task will be process in map/reduce state
	// also keep all worker when some work is still process in some of them
	if c.mapProcess.Len() > 0 || c.reduceProcess.Len() > 0 {
		reply.State = _wait
		return nil
	}
	reply.State = _end // All task done mark work done
	c.isDone = true
	return nil
}

func (c *Coordinator) HandleTaskReport(args *TaskState, reply *ExampleReply) error {
	switch args.State {
	case _map:
		fmt.Printf("[Coordinator] Map task on %vth file %v complete\n", args.MapIndex, args.FileName)
		c.mapProcess.RemoveTask(args.MapIndex, args.ReduceIndex)
		if c.mapProcess.Len() == 0 && c.mapIdle.Len() == 0 {
			//assign the task for reduce
			c.assignReduceTask()
		}
		break
	case _reduce:
		fmt.Printf("[Coordinator] Reduce task on %vth part complete\n", args.ReduceIndex)
		c.reduceProcess.RemoveTask(args.MapIndex, args.ReduceIndex)
		break
	case _failed: // when failed happen in worker pass the task
		fmt.Printf("[Coordinator] task with MapIndex:%v ReduceIndex:%v failed, coorinator pass it\n", args.MapIndex, args.ReduceIndex)
		if args.FileName == "" { // reduce task have no filename
			c.reduceProcess.RemoveTask(args.MapIndex, args.ReduceIndex)
		} else { // map task have file name
			c.mapProcess.RemoveTask(args.MapIndex, args.ReduceIndex)
		}

	default:
		panic("[Coordinator] Task Done error")
	}
	return nil
}

func (c *Coordinator) assignReduceTask() {
	for i := 0; i < c.R; i++ {
		reduceTask := Task{
			MapIndex:    0,
			ReduceIndex: i,
			FileName:    "",
			R:           c.R,
			M:           len(c.fileList),
		}
		reduceTask.Now()

		c.reduceIdle.Push(reduceTask)
		//reduceTask.Log("AssignReduce")
	}
}

// assignMapTask initialize map work
func (c *Coordinator) assignMapTask() {
	mapTaskQueue := make([]Task, 0)
	for fileIndex, file := range c.fileList {
		mapTask := Task{
			MapIndex:    fileIndex,
			ReduceIndex: 0,
			FileName:    file,
			R:           c.R,
			M:           len(c.fileList), // total map amount is the amount of fileList
		}
		mapTask.Now()
		mapTaskQueue = append(mapTaskQueue, mapTask)
		//mapTask.Log("AssignMap")
	}
	c.mapIdle = TaskQueue{queue: mapTaskQueue}
}

func withTmpDir() {
	// create tmp directory if not exists
	if _, err := os.Stat("mr-tmp"); os.IsNotExist(err) {
		err = os.Mkdir("mr-tmp", os.ModePerm)
		if err != nil {
			fmt.Print("[Coordinator] Create tmp directory failed... Error: %v\n", err)
			panic("Create tmp directory failed...")
		}
	}
}

func (c *Coordinator) dealTimeOut() {
	for {
		time.Sleep(5 * time.Second)
		mapTask := c.mapProcess.PopTimeOut()
		if len(mapTask) > 0 {
			for _, v := range mapTask {
				c.mapIdle.Push(v)
			}
			mapTask = nil
		}
		mapReduce := c.reduceProcess.PopTimeOut()
		if len(mapReduce) > 0 {
			for _, v := range mapReduce {
				c.reduceIdle.Push(v)
			}
			mapReduce = nil
		}
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	//ret := false

	return c.isDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	//withTmpDir()

	c := Coordinator{
		fileList: files,
		R:        nReduce,
		isDone:   false,
	}

	c.assignMapTask() // push map task to mapIdle

	fmt.Println("[Coordinator] coordinator initialized")

	// watch timeout task
	go c.dealTimeOut()

	c.server()
	return &c
}
