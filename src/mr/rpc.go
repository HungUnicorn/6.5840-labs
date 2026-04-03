package mr

type TaskPhase int

const (
	MapPhase TaskPhase = iota
	ReducePhase
	WaitPhase
	ExitPhase
)

type TaskRequest struct{}

type TaskResponse struct {
	Phase    TaskPhase
	TaskId   int
	FileName string
	NReduce  int
	NMap     int
}

type ReportTaskRequest struct {
	Phase  TaskPhase
	TaskId int
}

type ReportTaskResponse struct{}
