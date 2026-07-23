package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	"sync"
	"time"

	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader

	HeartbeatInterval    = 100 * time.Millisecond
	ElectionTimeoutBase  = 300
	ElectionTimeoutRange = 300
)

type LogEntry struct {
	ElectionTerm int
	Command      interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Persistent state
	currentTerm   int
	votedFor      int
	logEntries    []LogEntry
	snapshotIndex int

	// Volatile state on all servers
	role                  Role
	lastHeartbeat         time.Time
	highestCommittedIndex int
	highestAppliedIndex   int

	nextLogIndexToSend     []int
	highestReplicatedIndex []int

	applyCh   chan raftapi.ApplyMsg
	applyCond *sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return -1, -1, false
	}

	newLogEntry := LogEntry{
		ElectionTerm: rf.currentTerm,
		Command:      command,
	}

	rf.logEntries = append(rf.logEntries, newLogEntry)
	rf.persist()

	go rf.broadcastAppendEntries()

	return rf.getLastLogIndex(), rf.currentTerm, true
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (3A, 3B, 3C).
	rf.mu.Lock()
	rf.role = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.lastHeartbeat = time.Now()
	dummyLogEntry := LogEntry{}
	rf.logEntries = []LogEntry{dummyLogEntry}
	rf.highestCommittedIndex = 0
	rf.highestAppliedIndex = 0
	rf.snapshotIndex = 0
	rf.mu.Unlock()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.applyCond = sync.NewCond(&rf.mu)

	go rf.ticker()
	go rf.applier()

	return rf
}
