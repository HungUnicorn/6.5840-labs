package raft

import (
	"testing"

	"6.5840/raftapi"
	tester "6.5840/tester1"
)

func initializeDummySnapshot(persister *tester.Persister) {
	persister.Save([]byte("dummy"), []byte("snapdata"))
}

func TestEncodeDecodeRaftState(t *testing.T) {
	rf := makeTestRaft([]LogEntry{entry(0), entry(1)})
	rf.currentTerm = 5
	rf.votedFor = 2
	rf.snapshotIndex = 3

	encoded := rf.encodeRaftState()

	rf2 := makeTestRaft([]LogEntry{})
	rf2.readPersist(encoded)

	if rf2.currentTerm != 5 {
		t.Errorf("Decoded term = %d, want 5", rf2.currentTerm)
	}
	if rf2.votedFor != 2 {
		t.Errorf("Decoded votedFor = %d, want 2", rf2.votedFor)
	}
	if rf2.snapshotIndex != 3 {
		t.Errorf("Decoded snapshotIndex = %d, want 3", rf2.snapshotIndex)
	}
	if len(rf2.logEntries) != 2 || rf2.logEntries[1].ElectionTerm != 1 {
		t.Errorf("Decoded log entries mismatch: %v", rf2.logEntries)
	}
	if rf2.highestCommittedIndex != 3 || rf2.highestAppliedIndex != 3 {
		t.Errorf("Decoded highestCommitted/Applied mismatch: commit=%d, apply=%d", rf2.highestCommittedIndex, rf2.highestAppliedIndex)
	}
}

func TestReadPersistNilData(t *testing.T) {
	rf := makeTestRaft([]LogEntry{entry(0)})
	rf.currentTerm = 2
	rf.votedFor = 1

	rf.readPersist(nil)
	rf.readPersist([]byte{})

	stateRemainedUnmodified := rf.currentTerm == 2 && rf.votedFor == 1
	if !stateRemainedUnmodified {
		t.Errorf("readPersist with nil/empty data modified state")
	}
}

func TestPersist(t *testing.T) {
	rf := makeTestRaft([]LogEntry{entry(0)})
	rf.currentTerm = 3
	rf.votedFor = 1

	initializeDummySnapshot(rf.persister)

	rf.persist()

	savedState := rf.persister.ReadRaftState()
	savedSnap := rf.persister.ReadSnapshot()

	if len(savedState) == 0 {
		t.Errorf("Expected Raft state to be saved")
	}
	if string(savedSnap) != "snapdata" {
		t.Errorf("Expected Snapshot to be maintained, got %v", string(savedSnap))
	}
}

func TestPersistSnapshot(t *testing.T) {
	rf := makeTestRaft([]LogEntry{entry(0)})
	rf.currentTerm = 3
	rf.votedFor = 1

	rf.persistSnapshot([]byte("newsnapdata"))

	savedState := rf.persister.ReadRaftState()
	savedSnap := rf.persister.ReadSnapshot()

	if len(savedState) == 0 {
		t.Errorf("Expected Raft state to be saved")
	}
	if string(savedSnap) != "newsnapdata" {
		t.Errorf("Expected Snapshot to be updated, got %v", string(savedSnap))
	}
}

func TestPersistBytes(t *testing.T) {
	rf := makeTestRaft([]LogEntry{entry(0)})

	if rf.PersistBytes() != 0 {
		t.Errorf("Expected 0 bytes initially, got %d", rf.PersistBytes())
	}

	rf.persist()

	if rf.PersistBytes() == 0 {
		t.Errorf("Expected non-zero bytes after persist")
	}
}

func TestSnapshot(t *testing.T) {
	rf := makeTestRaft([]LogEntry{entry(0), entry(1), entry(2), entry(3)})
	rf.snapshotIndex = 0

	rf.Snapshot(2, []byte("mysnapshot"))

	if rf.snapshotIndex != 2 {
		t.Errorf("snapshotIndex = %d, want 2", rf.snapshotIndex)
	}
	if len(rf.logEntries) != 2 {
		t.Fatalf("log length = %d, want 2", len(rf.logEntries))
	}

	savedSnap := rf.persister.ReadSnapshot()
	if string(savedSnap) != "mysnapshot" {
		t.Errorf("Snapshot data mismatch")
	}

	rf.Snapshot(1, []byte("oldsnapshot"))
	snapshotRegressed := rf.snapshotIndex != 2
	if snapshotRegressed {
		t.Errorf("Snapshot regressed to older index")
	}
	snapshotDataOverwritten := string(rf.persister.ReadSnapshot()) != "mysnapshot"
	if snapshotDataOverwritten {
		t.Errorf("Snapshot data was overwritten by stale snapshot request")
	}
}

func TestCommitSnapshot(t *testing.T) {
	applyCh := make(chan raftapi.ApplyMsg, 1)
	rf := makeTestRaft([]LogEntry{entry(0), entry(1), entry(2)})
	rf.applyCh = applyCh
	rf.mu.Lock()

	rf.commitSnapshot(5, 5, []byte("installsnapshot"))

	rf.mu.Unlock()

	if rf.snapshotIndex != 5 {
		t.Errorf("snapshotIndex = %d, want 5", rf.snapshotIndex)
	}
	if len(rf.logEntries) != 1 {
		t.Errorf("log length = %d, want 1", len(rf.logEntries))
	}
	if string(rf.persister.ReadSnapshot()) != "installsnapshot" {
		t.Errorf("Snapshot data mismatch")
	}

	msg := <-applyCh
	if !msg.SnapshotValid || string(msg.Snapshot) != "installsnapshot" {
		t.Errorf("Invalid snapshot message on applyCh")
	}
}
