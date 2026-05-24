package raft

import (
	"bytes"
	"log"

	"6.5840/labgob"
)

func (rf *Raft) encodeRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.logEntries)
	return w.Bytes()
}

func (rf *Raft) persist() {
	rf.persister.Save(rf.encodeRaftState(), rf.persister.ReadSnapshot())
}

func (rf *Raft) persistSnapshot(snapshot []byte) {
	rf.persister.Save(rf.encodeRaftState(), snapshot)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var snapshotIndex int
	var logEntries []LogEntry

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&snapshotIndex) != nil ||
		d.Decode(&logEntries) != nil {
		log.Fatal("failed to decode persisted Raft state")
	}

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.snapshotIndex = snapshotIndex
	rf.logEntries = logEntries

	snapshotIsNewBaseIndex := snapshotIndex
	rf.highestCommittedIndex = snapshotIsNewBaseIndex
	rf.highestAppliedIndex = snapshotIsNewBaseIndex
}


func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}


func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.snapshotIndex {
		return
	}

	rf.truncateLogForLocalSnapshot(index)
	rf.persistSnapshot(snapshot)
}

func (rf *Raft) commitSnapshot(lastIncludedIndex int, lastIncludedTerm int, data []byte) {
	rf.truncateLogForInstallSnapshot(lastIncludedIndex, lastIncludedTerm)
	rf.advanceIndicesForSnapshot(lastIncludedIndex)
	rf.persistSnapshot(data)
	rf.pushSnapshotToService(lastIncludedIndex, lastIncludedTerm, data)
}
