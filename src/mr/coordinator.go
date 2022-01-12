package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskState string

type Task struct {
	filename        string
	taskType        CState
	assignationTime time.Time
}

type CState int

const (
	MAPPING CState = iota
	REDUCING
	FINISHED
)

type Coordinator struct {
	mutex   sync.Mutex
	state   CState
	nReduce int
	tasks   map[int]Task
}

const TaskTimeout = time.Minute * 10

func (c *Coordinator) RequestTask(args *RequestTask, reply *RequestTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	taskAssigned := false
	oldestTask := time.Now()
	tenMinAgo := time.Now().Add(-TaskTimeout)
	for k, task := range c.tasks {
		if task.assignationTime.Before(oldestTask) {
			oldestTask = task.assignationTime
		}

		// If task hasn't been finish in the last 10min
		// we reassigned it
		if task.assignationTime.Before(tenMinAgo) {
			taskAssigned = true

			reply.key = k
			reply.filename = task.filename
			reply.nReducer = c.nReduce
		}
	}
	if taskAssigned == false {
		reply.rtype = IDLE
		reply.idleTime =
	}

	return nil
}

func (c *Coordinator) MapTaskComplete(args *RequestTask, reply *RequestTaskReply) error {
	// If all task are completed, then we can
	if len(c.tasks) == 0 {
		switch c.state {
		case MAPPING:
			break
		case REDUCING:
			break
		case FINISHED:
			print("MapReduce complete, ")
			break
		}
	}

	return nil
}

func (c *Coordinator) ReduceTaskComplete(args *RequestTask, reply *RequestTaskReply) error {
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

func (c *Coordinator) Done() bool {
	return c.state == REDUCING && len(c.tasks) == 0
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce: nReduce,
		tasks:   make(map[int]Task),
	}

	// We defined an expired date to be sure that all task we'll be assigned
	expiredDate := time.Now().Add(-TaskTimeout)
	for i, file := range files {
		c.tasks[i] = Task{
			filename:        file,
			taskType:        MAPPING,
			assignationTime: expiredDate,
		}
	}

	c.server()
	return &c
}
