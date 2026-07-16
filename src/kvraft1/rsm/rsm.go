package rsm

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type Op struct {
	OriginServerId int
	OperationId    int64
	ClientRequest  any
}

type OperationResult struct {
	OperationId     int64
	ExecutionResult any
}


// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	
	operationIdCounter int64
	pendingOperations  map[int]chan OperationResult
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:                me,
		maxraftstate:      maxraftstate,
		applyCh:           make(chan raftapi.ApplyMsg),
		sm:                sm,
		pendingOperations: make(map[int]chan OperationResult),
	}
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		rsm.sm.Restore(snapshot)
	}
	go rsm.run()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

func (rsm *RSM) run() {
	for msg := range rsm.applyCh {
		if msg.CommandValid {
			rsm.handleCommand(msg)
		} else if msg.SnapshotValid {
			rsm.handleSnapshot(msg)
		}
	}
	rsm.cleanupPendingOperations()
}

func (rsm *RSM) handleCommand(msg raftapi.ApplyMsg) {
	op, ok := msg.Command.(Op)
	if !ok {
		return
	}

	executionResult := rsm.sm.DoOp(op.ClientRequest)

	rsm.notifyWaitingClient(msg.CommandIndex, op.OperationId, executionResult)

	if rsm.shouldTakeSnapshot() {
		rsm.takeSnapshot(msg.CommandIndex)
	}
}

func (rsm *RSM) notifyWaitingClient(index int, opId int64, result any) {
	rsm.mu.Lock()
	ch, exists := rsm.pendingOperations[index]
	rsm.mu.Unlock()

	if exists {
		ch <- OperationResult{
			OperationId:     opId,
			ExecutionResult: result,
		}
	}
}

func (rsm *RSM) shouldTakeSnapshot() bool {
	return rsm.maxraftstate != -1 && rsm.rf.PersistBytes() > rsm.maxraftstate
}

func (rsm *RSM) takeSnapshot(index int) {
	snapshot := rsm.sm.Snapshot()
	rsm.rf.Snapshot(index, snapshot)
}

func (rsm *RSM) handleSnapshot(msg raftapi.ApplyMsg) {
	if len(msg.Snapshot) > 0 {
		rsm.sm.Restore(msg.Snapshot)
	}
}

func (rsm *RSM) cleanupPendingOperations() {
	rsm.mu.Lock()
	for _, ch := range rsm.pendingOperations {
		close(ch)
	}
	rsm.mu.Unlock()
}


// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	rsm.mu.Lock()
	rsm.operationIdCounter++
	operationId := rsm.operationIdCounter
	rsm.mu.Unlock()

	op := Op{
		OriginServerId: rsm.me,
		OperationId:    operationId,
		ClientRequest:  req,
	}

	index, term, isLeader := rsm.rf.Start(op)
	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}

	rsm.mu.Lock()
	ch := make(chan OperationResult, 1)
	rsm.pendingOperations[index] = ch
	rsm.mu.Unlock()

	defer func() {
		rsm.mu.Lock()
		delete(rsm.pendingOperations, index)
		rsm.mu.Unlock()
	}()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case result, ok := <-ch:
			if !ok {
				// Server was shutdown/killed, pending channels closed
				return rpc.ErrWrongLeader, nil
			}
			if result.OperationId == operationId {
				return rpc.OK, result.ExecutionResult
			}
			return rpc.ErrWrongLeader, nil
		case <-ticker.C:
			currentTerm, isLeader := rsm.rf.GetState()
			if !isLeader || currentTerm != term {
				return rpc.ErrWrongLeader, nil
			}
		}
	}
}
