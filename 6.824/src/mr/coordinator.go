package mr

import (
	"fmt"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskStatus int

const (
	NotStart TaskStatus = iota
	Doing
	Done
)

type Coordinator struct {
	// Your definitions here.

	mutex sync.Mutex

	mapNum          int
	reduceNum       int
	mapTasks        map[int]TaskStatus
	reduceTasks     map[int]TaskStatus
	currentPhase    TaskPhase
	inputFiles      []string
	taskDone        bool
	doneMapTasks    int
	doneReduceTasks int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Request(args *CallArgs, reply *CallReply) error {
	switch args.CurrentStatus {
	case Idle:
		if c.taskDone {
			// todo fix：任务全部完成也需要通知 worker，而不能直接 return，避免未知错误
			reply.TaskDone = true
			return nil
		}
		switch c.currentPhase {
		case MapPhase:
			return c.handleMapRequest(args, reply)
		case ReducePhase:
			return c.handleReduceRequest(args, reply)
		}
	case Finished:
		return c.handleFinished(args, reply)
	case Failed:
		return c.handleFailed(args, reply)
	default:
		return fmt.Errorf("invalid worker (%s %d) with %s status", c.currentPhase.String(), args.TaskID, args.CurrentStatus.String())
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.taskDone {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.inputFiles = files
	c.mapNum = len(files)
	c.reduceNum = nReduce
	c.doneMapTasks = 0
	c.doneReduceTasks = 0

	c.mapTasks = make(map[int]TaskStatus, c.mapNum)
	for tid := range files {
		c.mapTasks[int(tid)] = NotStart
	}

	c.reduceTasks = make(map[int]TaskStatus, c.reduceNum)
	for tid := range files {
		c.reduceTasks[int(tid)] = NotStart
	}

	c.server()
	return &c
}
