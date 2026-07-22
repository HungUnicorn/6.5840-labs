package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var errNoAvailableTask = errors.New("no available task")

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

	switch c.phase {
	case MapPhase:
		if err := c.tryAssignAvailableTask(c.mapTasks, DoMap, reply); err == nil {
			return nil
		}
		reply.Directive = Wait
	case ReducePhase:
		if err := c.tryAssignAvailableTask(c.reduceTasks, DoReduce, reply); err == nil {
			return nil
		}
		reply.Directive = Wait
	case DonePhase:
		reply.Directive = Exit
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

func (c *Coordinator) tryAssignAvailableTask(tasks []Task, directive WorkerDirective, reply *TaskResponse) error {
	for i := range tasks {
		task := &tasks[i]

		isIdle := task.Status == Idle
		isTimedOut := task.Status == InProgress && time.Since(task.StartTime) > TaskTimeout

		if isIdle || isTimedOut {
			task.Status = InProgress
			task.StartTime = time.Now()

			reply.Directive = directive
			reply.TaskId = i
			reply.FileName = task.File
			reply.NReduce = c.nReduce
			reply.NMap = c.nMap

			return nil
		}
	}
	return errNoAvailableTask
}

func (c *Coordinator) markTaskCompleted(tasks []Task, taskId int) {
	if tasks[taskId].Status != InProgress {
		return
	}

	tasks[taskId].Status = Completed
	c.checkPhaseTransition()
}

func (c *Coordinator) checkPhaseTransition() {
	prev := c.phase

	switch c.phase {
	case MapPhase:
		if c.isAllComplete(c.mapTasks) {
			c.phase = ReducePhase
		}
	case ReducePhase:
		if c.isAllComplete(c.reduceTasks) {
			c.phase = DonePhase
		}
	}

	if c.phase != prev {
		log.Printf("All %v tasks complete. Transitioning to %v Phase.", prev, c.phase)
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
	return c.phase == DonePhase
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
