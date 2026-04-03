package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const TaskTimeout = 10 * time.Second

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

type Task struct {
	Status    TaskStatus
	StartTime time.Time
	File      string // Only used for Map tasks
}

type Coordinator struct {
	mu          sync.Mutex
	phase       TaskPhase
	mapTasks    []Task
	reduceTasks []Task
	nMap        int
	nReduce     int
}

// ---------------------------------------------------------
// RPC Handlers
// ---------------------------------------------------------

func (c *Coordinator) GetTask(args *TaskRequest, reply *TaskResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.phase == MapPhase && c.assignTask(c.mapTasks, MapPhase, reply) {
		return nil
	}

	if c.phase == ReducePhase && c.assignTask(c.reduceTasks, ReducePhase, reply) {
		return nil
	}

	if c.phase == ExitPhase {
		reply.Phase = ExitPhase
	} else {
		reply.Phase = WaitPhase
	}

	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskRequest, reply *ReportTaskResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.Phase != c.phase {
		return nil
	}

	if c.phase == MapPhase {
		c.markTaskCompleted(c.mapTasks, args.TaskId)
	} else if c.phase == ReducePhase {
		c.markTaskCompleted(c.reduceTasks, args.TaskId)
	}

	return nil
}

// ---------------------------------------------------------
// Private Helper Methods
// ---------------------------------------------------------

func (c *Coordinator) assignTask(tasks []Task, phase TaskPhase, reply *TaskResponse) bool {
	for i := range tasks {
		task := &tasks[i]

		isIdle := task.Status == Idle
		isTimedOut := task.Status == InProgress && time.Since(task.StartTime) > TaskTimeout

		if isIdle || isTimedOut {
			task.Status = InProgress
			task.StartTime = time.Now()

			reply.Phase = phase
			reply.TaskId = i
			reply.FileName = task.File
			reply.NReduce = c.nReduce
			reply.NMap = c.nMap

			return true
		}
	}
	return false
}

func (c *Coordinator) markTaskCompleted(tasks []Task, taskId int) {
	if tasks[taskId].Status != InProgress {
		return
	}

	tasks[taskId].Status = Completed
	c.checkPhaseTransition()
}

func (c *Coordinator) checkPhaseTransition() {
	if c.phase == MapPhase && c.isAllComplete(c.mapTasks) {
		log.Println("All Map tasks complete. Transitioning to Reduce Phase.")
		c.phase = ReducePhase
	} else if c.phase == ReducePhase && c.isAllComplete(c.reduceTasks) {
		log.Println("All Reduce tasks complete. Transitioning to Exit Phase.")
		c.phase = ExitPhase
	}
}

func (c *Coordinator) isAllComplete(tasks []Task) bool {
	for _, task := range tasks {
		if task.Status != Completed {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------
// Lifecycle and Bootstrapping
// ---------------------------------------------------------

func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase == ExitPhase
}

func initCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		phase:       MapPhase,
		nMap:        len(files),
		nReduce:     nReduce,
		mapTasks:    make([]Task, len(files)),
		reduceTasks: make([]Task, nReduce),
	}

	for i, file := range files {
		c.mapTasks[i] = Task{
			Status: Idle,
			File:   file,
		}
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{Status: Idle}
	}
	return &c
}

func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := initCoordinator(sockname, files, nReduce)
	c.server(sockname)

	return c
}
