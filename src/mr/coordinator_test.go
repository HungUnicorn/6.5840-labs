package mr

import (
	"testing"
	"time"
)

func TestGetTask_IdleMapTask_AssignsTaskAndSetsInProgress(t *testing.T) {
	files := []string{"file1.txt", "file2.txt"}
	c := initCoordinator("dummy-socket", files, 1)
	reply := TaskResponse{}

	c.GetTask(&TaskRequest{}, &reply)

	if reply.Phase != MapPhase {
		t.Errorf("Expected MapPhase, got %v", reply.Phase)
	}
	if reply.TaskId != 0 {
		t.Errorf("Expected TaskId 0, got %v", reply.TaskId)
	}
	if c.mapTasks[0].Status != InProgress {
		t.Errorf("Expected Map Task 0 to be InProgress")
	}
}

func TestGetTask_MismatchedCounts_AssignsCorrectIndices(t *testing.T) {
	files := []string{"f1.txt", "f2.txt"}
	c := initCoordinator("test-sock", files, 3)

	for i := 0; i < 2; i++ {
		reply := TaskResponse{}
		c.GetTask(&TaskRequest{}, &reply)
		if reply.Phase != MapPhase || reply.TaskId != i {
			t.Errorf("Expected Map Task %v, got %v Phase %v", i, reply.TaskId, reply.Phase)
		}
	}

	reply := TaskResponse{}
	c.GetTask(&TaskRequest{}, &reply)
	if reply.Phase != WaitPhase {
		t.Errorf("Expected WaitPhase, got %v", reply.Phase)
	}
}

func TestGetTask_MapTaskTimedOut_ReassignsToNewWorker(t *testing.T) {
	files := []string{"f1.txt"}
	c := initCoordinator("test-sock", files, 1)

	reply1 := TaskResponse{}
	c.GetTask(&TaskRequest{}, &reply1)

	c.mu.Lock()
	c.mapTasks[0].StartTime = time.Now().Add(-11 * time.Second)
	c.mu.Unlock()

	reply2 := TaskResponse{}
	c.GetTask(&TaskRequest{}, &reply2)

	if reply2.TaskId != 0 || reply2.Phase != MapPhase {
		t.Errorf("Failed to reassign timed-out task")
	}
}

func TestGetTask_ReduceTaskTimedOut_ReassignsToNewWorker(t *testing.T) {
	files := []string{"f1.txt"}
	c := initCoordinator("test-sock", files, 1)

	c.mapTasks[0].Status = Completed
	c.phase = ReducePhase

	reply1 := TaskResponse{}
	c.GetTask(&TaskRequest{}, &reply1)

	c.mu.Lock()
	c.reduceTasks[0].StartTime = time.Now().Add(-11 * time.Second)
	c.mu.Unlock()

	reply2 := TaskResponse{}
	c.GetTask(&TaskRequest{}, &reply2)

	if reply2.TaskId != 0 || reply2.Phase != ReducePhase {
		t.Errorf("Failed to reassign timed-out reduce task")
	}
}

func TestReportTask_DuplicateMapReport_DoesNotTransitionPrematurely(t *testing.T) {
	files := []string{"f1.txt", "f2.txt"}
	c := initCoordinator("test-sock", files, 1)

	c.mapTasks[0].Status = InProgress
	c.ReportTask(&ReportTaskRequest{Phase: MapPhase, TaskId: 0}, &ReportTaskResponse{})
	c.ReportTask(&ReportTaskRequest{Phase: MapPhase, TaskId: 0}, &ReportTaskResponse{})

	if c.phase == ReducePhase {
		t.Errorf("Transitioned to ReducePhase while task 1 is idle")
	}
}

func TestReportTask_AllTasksFinished_TransitionsToExitPhase(t *testing.T) {
	files := []string{"f1.txt"}
	c := initCoordinator("test-sock", files, 1)

	c.mapTasks[0].Status = InProgress
	c.ReportTask(&ReportTaskRequest{Phase: MapPhase, TaskId: 0}, &ReportTaskResponse{})

	if c.phase != ReducePhase {
		t.Errorf("Expected ReducePhase")
	}

	c.reduceTasks[0].Status = InProgress
	c.ReportTask(&ReportTaskRequest{Phase: ReducePhase, TaskId: 0}, &ReportTaskResponse{})

	if c.phase != ExitPhase {
		t.Errorf("Expected ExitPhase")
	}

	if !c.Done() {
		t.Errorf("Done() should be true")
	}
}

func TestReportTask_StaleMapReportInReducePhase_IsIgnored(t *testing.T) {
	files := []string{"f1.txt"}
	c := initCoordinator("test-sock", files, 1)

	c.phase = ReducePhase
	c.reduceTasks[0].Status = Idle

	c.ReportTask(&ReportTaskRequest{Phase: MapPhase, TaskId: 0}, &ReportTaskResponse{})

	if c.reduceTasks[0].Status == Completed {
		t.Errorf("Stale report modified reduce task status")
	}
}

func TestReportTask_AllMapTasksCompleted_TransitionsToReducePhase(t *testing.T) {
	files := []string{"file1.txt", "file2.txt"}
	c := initCoordinator("dummy-socket", files, 2)

	c.mapTasks[0].Status = InProgress
	c.mapTasks[1].Status = InProgress

	c.ReportTask(&ReportTaskRequest{Phase: MapPhase, TaskId: 0}, &ReportTaskResponse{})

	if c.phase != MapPhase {
		t.Errorf("Coordinator transitioned to ReducePhase prematurely")
	}

	c.ReportTask(&ReportTaskRequest{Phase: MapPhase, TaskId: 1}, &ReportTaskResponse{})

	if c.phase != ReducePhase {
		t.Errorf("Coordinator failed to transition to ReducePhase after all maps finished")
	}
}
