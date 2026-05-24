package raft

import (
	"sync"
	"testing"

	"6.5840/labrpc"
	tester "6.5840/tester1"
)

func makeTestRaftWithPeers(logEntries []LogEntry, me int, peerCount int) *Raft {
	rf := &Raft{
		logEntries: logEntries,
		persister:  tester.MakePersister(),
		me:                     me,
		peers:                  make([]*labrpc.ClientEnd, peerCount),
		votedFor:               -1,
		nextLogIndexToSend:     make([]int, peerCount),
		highestReplicatedIndex: make([]int, peerCount),
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	return rf
}

func TestBecomeFollower(t *testing.T) {
	rf := makeTestRaftWithPeers([]LogEntry{entry(0)}, 0, 3)
	rf.role = Leader
	rf.currentTerm = 5
	rf.votedFor = 0

	rf.becomeFollower(10)

	if rf.role != Follower {
		t.Errorf("role = %v, want Follower", rf.role)
	}
	if rf.currentTerm != 10 {
		t.Errorf("currentTerm = %d, want 10", rf.currentTerm)
	}
	if rf.votedFor != -1 {
		t.Errorf("votedFor = %d, want -1", rf.votedFor)
	}
}

func TestBecomeCandidate(t *testing.T) {
	rf := makeTestRaftWithPeers([]LogEntry{entry(0)}, 2, 5)
	rf.role = Follower
	rf.currentTerm = 3
	rf.votedFor = -1

	rf.becomeCandidate()

	if rf.role != Candidate {
		t.Errorf("role = %v, want Candidate", rf.role)
	}
	if rf.currentTerm != 4 {
		t.Errorf("currentTerm = %d, want 4", rf.currentTerm)
	}
	if rf.votedFor != 2 {
		t.Errorf("votedFor = %d, want 2 (self)", rf.votedFor)
	}
}

func TestBecomeLeader(t *testing.T) {
	network := labrpc.MakeNetwork()
	defer network.Cleanup()

	peerCount := 3
	ends := make([]*labrpc.ClientEnd, peerCount)
	for i := 0; i < peerCount; i++ {
		ends[i] = network.MakeEnd("peer" + string(rune('0'+i)))
	}

	rf := &Raft{
		logEntries: []LogEntry{entry(0), entry(1), entry(2)},
		persister:  tester.MakePersister(),
		me:         0,
		peers:      ends,
		votedFor:   -1,
		role:       Candidate,
		currentTerm: 2,
	}
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.becomeLeader()

	if rf.role != Leader {
		t.Errorf("role = %v, want Leader", rf.role)
	}
	for i, nextIdx := range rf.nextLogIndexToSend {
		if nextIdx != 3 {
			t.Errorf("nextLogIndexToSend[%d] = %d, want 3", i, nextIdx)
		}
	}
	for i, matchIdx := range rf.highestReplicatedIndex {
		if matchIdx != 0 {
			t.Errorf("highestReplicatedIndex[%d] = %d, want 0", i, matchIdx)
		}
	}
}

func TestRequestVote(t *testing.T) {
	tests := []struct {
		name            string
		serverTerm      int
		serverVotedFor  int
		serverLog       []LogEntry
		argsTerm        int
		argsCandidateId int
		argsLastLogTerm int
		argsLastLogIdx  int
		wantGranted     bool
		wantTerm        int
		wantVotedFor    int
	}{
		{
			name:            "grant vote to up-to-date candidate",
			serverTerm:      1,
			serverVotedFor:  -1,
			serverLog:       []LogEntry{entry(0), entry(1)},
			argsTerm:        1,
			argsCandidateId: 2,
			argsLastLogTerm: 1,
			argsLastLogIdx:  1,
			wantGranted:     true,
			wantTerm:        1,
			wantVotedFor:    2,
		},
		{
			name:            "deny vote for stale term",
			serverTerm:      5,
			serverVotedFor:  -1,
			serverLog:       []LogEntry{entry(0), entry(1)},
			argsTerm:        3,
			argsCandidateId: 1,
			argsLastLogTerm: 1,
			argsLastLogIdx:  1,
			wantGranted:     false,
			wantTerm:        5,
			wantVotedFor:    -1,
		},
		{
			name:            "deny vote when already voted for another",
			serverTerm:      2,
			serverVotedFor:  1,
			serverLog:       []LogEntry{entry(0), entry(1)},
			argsTerm:        2,
			argsCandidateId: 3,
			argsLastLogTerm: 2,
			argsLastLogIdx:  5,
			wantGranted:     false,
			wantTerm:        2,
			wantVotedFor:    1,
		},
		{
			name:            "grant vote to same candidate again",
			serverTerm:      2,
			serverVotedFor:  3,
			serverLog:       []LogEntry{entry(0), entry(1)},
			argsTerm:        2,
			argsCandidateId: 3,
			argsLastLogTerm: 1,
			argsLastLogIdx:  1,
			wantGranted:     true,
			wantTerm:        2,
			wantVotedFor:    3,
		},
		{
			name:            "deny vote when candidate log is stale",
			serverTerm:      2,
			serverVotedFor:  -1,
			serverLog:       []LogEntry{entry(0), entry(1), entry(2)},
			argsTerm:        2,
			argsCandidateId: 1,
			argsLastLogTerm: 1,
			argsLastLogIdx:  3,
			wantGranted:     false,
			wantTerm:        2,
			wantVotedFor:    -1,
		},
		{
			name:            "step down and grant vote on higher term",
			serverTerm:      2,
			serverVotedFor:  0,
			serverLog:       []LogEntry{entry(0), entry(1)},
			argsTerm:        5,
			argsCandidateId: 1,
			argsLastLogTerm: 1,
			argsLastLogIdx:  1,
			wantGranted:     true,
			wantTerm:        5,
			wantVotedFor:    1,
		},
		{
			name:            "step down but deny vote if candidate log is stale",
			serverTerm:      2,
			serverVotedFor:  0,
			serverLog:       []LogEntry{entry(0), entry(1), entry(2)},
			argsTerm:        5,
			argsCandidateId: 1,
			argsLastLogTerm: 1,
			argsLastLogIdx:  1,
			wantGranted:     false,
			wantTerm:        5,
			wantVotedFor:    -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rf := makeTestRaftWithPeers(tt.serverLog, 0, 5)
			rf.currentTerm = tt.serverTerm
			rf.votedFor = tt.serverVotedFor
			rf.role = Follower

			args := &RequestVoteArgs{
				Term:         tt.argsTerm,
				CandidateId:  tt.argsCandidateId,
				LastLogTerm:  tt.argsLastLogTerm,
				LastLogIndex: tt.argsLastLogIdx,
			}
			reply := &RequestVoteReply{}

			rf.RequestVote(args, reply)

			if reply.VoteGranted != tt.wantGranted {
				t.Errorf("VoteGranted = %v, want %v", reply.VoteGranted, tt.wantGranted)
			}
			if reply.Term != tt.wantTerm {
				t.Errorf("reply.Term = %d, want %d", reply.Term, tt.wantTerm)
			}
			if rf.votedFor != tt.wantVotedFor {
				t.Errorf("votedFor = %d, want %d", rf.votedFor, tt.wantVotedFor)
			}
		})
	}
}
