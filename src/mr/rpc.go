package mr

import "fmt"

type TaskPhase int

const (
	MapPhase TaskPhase = iota
	ReducePhase
	DonePhase
)

func (p TaskPhase) String() string {
	switch p {
	case MapPhase:
		return "Map"
	case ReducePhase:
		return "Reduce"
	case DonePhase:
		return "Done"
	default:
		return fmt.Sprintf("TaskPhase(%d)", int(p))
	}
}

type WorkerDirective int

const (
	DoMap WorkerDirective = iota
	DoReduce
	Wait
	Exit
)

func (d WorkerDirective) String() string {
	switch d {
	case DoMap:
		return "DoMap"
	case DoReduce:
		return "DoReduce"
	case Wait:
		return "Wait"
	case Exit:
		return "Exit"
	default:
		return fmt.Sprintf("WorkerDirective(%d)", int(d))
	}
}

type TaskRequest struct{}

type TaskResponse struct {
	Directive WorkerDirective
	TaskId    int
	FileName  string
	NReduce   int
	NMap      int
}

type ReportTaskRequest struct {
	Phase  TaskPhase
	TaskId int
}

type ReportTaskResponse struct{}
