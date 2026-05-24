package raft

import (
	"sync"
	"testing"

	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

func makeTestRaft(logEntries []LogEntry) *Raft {
	rf := &Raft{
		logEntries: logEntries,
		persister:  tester.MakePersister(),
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	return rf
}

func entry(term int) LogEntry {
	return LogEntry{ElectionTerm: term}
}

func TestGetLatestLogIndex(t *testing.T) {
	rf := makeTestRaft([]LogEntry{entry(0), entry(1), entry(1), entry(2)})
	if got := rf.getLatestLogIndex(); got != 3 {
		t.Errorf("getLatestLogIndex() = %d, want 3", got)
	}
}

func TestGetLatestLogTerm(t *testing.T) {
	rf := makeTestRaft([]LogEntry{entry(0), entry(1), entry(2), entry(5)})
	if got := rf.getLatestLogTerm(); got != 5 {
		t.Errorf("getLatestLogTerm() = %d, want 5", got)
	}
}

func TestIsCandidateLogUpToDate(t *testing.T) {
	tests := []struct {
		name               string
		myLog              []LogEntry
		candidateLastTerm  int
		candidateLastIndex int
		want               bool
	}{
		{
			name:               "candidate has higher term",
			myLog:              []LogEntry{entry(0), entry(1)},
			candidateLastTerm:  2,
			candidateLastIndex: 1,
			want:               true,
		},
		{
			name:               "candidate has lower term",
			myLog:              []LogEntry{entry(0), entry(3)},
			candidateLastTerm:  2,
			candidateLastIndex: 5,
			want:               false,
		},
		{
			name:               "same term candidate has longer log",
			myLog:              []LogEntry{entry(0), entry(1)},
			candidateLastTerm:  1,
			candidateLastIndex: 3,
			want:               true,
		},
		{
			name:               "same term candidate has shorter log",
			myLog:              []LogEntry{entry(0), entry(1), entry(1), entry(1)},
			candidateLastTerm:  1,
			candidateLastIndex: 1,
			want:               false,
		},
		{
			name:               "same term same length",
			myLog:              []LogEntry{entry(0), entry(1), entry(1)},
			candidateLastTerm:  1,
			candidateLastIndex: 2,
			want:               true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rf := makeTestRaft(tt.myLog)
			got := rf.isCandidateLogUpToDate(tt.candidateLastTerm, tt.candidateLastIndex)
			if got != tt.want {
				t.Errorf("isCandidateLogUpToDate(%d, %d) = %v, want %v",
					tt.candidateLastTerm, tt.candidateLastIndex, got, tt.want)
			}
		})
	}
}

func TestFindFirstIndexOfTerm(t *testing.T) {
	tests := []struct {
		name       string
		log        []LogEntry
		term       int
		startIndex int
		want       int
	}{
		{
			name:       "single entry of term",
			log:        []LogEntry{entry(0), entry(1), entry(2), entry(3)},
			term:       2,
			startIndex: 2,
			want:       2,
		},
		{
			name:       "multiple entries of same term",
			log:        []LogEntry{entry(0), entry(1), entry(2), entry(2), entry(2), entry(3)},
			term:       2,
			startIndex: 4,
			want:       2,
		},
		{
			name:       "term starts at index 1",
			log:        []LogEntry{entry(0), entry(1), entry(1), entry(1)},
			term:       1,
			startIndex: 3,
			want:       1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rf := makeTestRaft(tt.log)
			got := rf.findFirstIndexOfTerm(tt.term, tt.startIndex)
			if got != tt.want {
				t.Errorf("findFirstIndexOfTerm(%d, %d) = %d, want %d",
					tt.term, tt.startIndex, got, tt.want)
			}
		})
	}
}

func TestCheckLogConsistency(t *testing.T) {
	tests := []struct {
		name              string
		log               []LogEntry
		prevLogIndex      int
		prevLogTerm       int
		wantOk            bool
		wantConflictTerm  int
		wantConflictIndex int
	}{
		{
			name:         "matching entry",
			log:          []LogEntry{entry(0), entry(1), entry(2)},
			prevLogIndex: 1,
			prevLogTerm:  1,
			wantOk:       true,
		},
		{
			name:              "log too short",
			log:               []LogEntry{entry(0), entry(1)},
			prevLogIndex:      5,
			prevLogTerm:       1,
			wantOk:            false,
			wantConflictTerm:  -1,
			wantConflictIndex: -1,
		},
		{
			name:              "term mismatch",
			log:               []LogEntry{entry(0), entry(1), entry(2), entry(2)},
			prevLogIndex:      2,
			prevLogTerm:       99,
			wantOk:            false,
			wantConflictTerm:  2,
			wantConflictIndex: 2,
		},
		{
			name:              "term mismatch skips entire conflicting term",
			log:               []LogEntry{entry(0), entry(1), entry(3), entry(3), entry(3)},
			prevLogIndex:      4,
			prevLogTerm:       99,
			wantOk:            false,
			wantConflictTerm:  3,
			wantConflictIndex: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rf := makeTestRaft(tt.log)
			args := &AppendEntriesArgs{
				IndexBeforeNewEntries: tt.prevLogIndex,
				TermBeforeNewEntries:  tt.prevLogTerm,
			}
			reply := &AppendEntriesReply{}

			got := rf.checkLogConsistency(args, reply)
			if got != tt.wantOk {
				t.Errorf("checkLogConsistency() = %v, want %v", got, tt.wantOk)
			}
			if !tt.wantOk {
				if reply.ConflictTerm != tt.wantConflictTerm {
					t.Errorf("ConflictTerm = %d, want %d", reply.ConflictTerm, tt.wantConflictTerm)
				}
				if reply.ConflictIndex != tt.wantConflictIndex {
					t.Errorf("ConflictIndex = %d, want %d", reply.ConflictIndex, tt.wantConflictIndex)
				}
			}
		})
	}
}

func TestMergeLogEntries(t *testing.T) {
	tests := []struct {
		name           string
		existingLog    []LogEntry
		indexBeforeNew int
		newEntries     []LogEntry
		wantLog        []LogEntry
	}{
		{
			name:           "append to end",
			existingLog:    []LogEntry{entry(0), entry(1)},
			indexBeforeNew: 1,
			newEntries:     []LogEntry{entry(2), entry(2)},
			wantLog:        []LogEntry{entry(0), entry(1), entry(2), entry(2)},
		},
		{
			name:           "no new entries",
			existingLog:    []LogEntry{entry(0), entry(1), entry(2)},
			indexBeforeNew: 0,
			newEntries:     []LogEntry{},
			wantLog:        []LogEntry{entry(0), entry(1), entry(2)},
		},
		{
			name:           "matching entries are kept",
			existingLog:    []LogEntry{entry(0), entry(1), entry(2), entry(3)},
			indexBeforeNew: 0,
			newEntries:     []LogEntry{entry(1), entry(2)},
			wantLog:        []LogEntry{entry(0), entry(1), entry(2), entry(3)},
		},
		{
			name:           "conflict truncates and appends",
			existingLog:    []LogEntry{entry(0), entry(1), entry(2), entry(3)},
			indexBeforeNew: 1,
			newEntries:     []LogEntry{entry(4), entry(5)},
			wantLog:        []LogEntry{entry(0), entry(1), entry(4), entry(5)},
		},
		{
			name:           "conflict in the middle",
			existingLog:    []LogEntry{entry(0), entry(1), entry(2), entry(3), entry(4)},
			indexBeforeNew: 1,
			newEntries:     []LogEntry{entry(2), entry(99)},
			wantLog:        []LogEntry{entry(0), entry(1), entry(2), entry(99)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			existingCopy := make([]LogEntry, len(tt.existingLog))
			copy(existingCopy, tt.existingLog)

			rf := makeTestRaft(existingCopy)
			rf.mergeLogEntries(tt.indexBeforeNew, tt.newEntries)

			if len(rf.logEntries) != len(tt.wantLog) {
				t.Fatalf("log length = %d, want %d", len(rf.logEntries), len(tt.wantLog))
			}
			for i, e := range rf.logEntries {
				if e.ElectionTerm != tt.wantLog[i].ElectionTerm {
					t.Errorf("log[%d].ElectionTerm = %d, want %d", i, e.ElectionTerm, tt.wantLog[i].ElectionTerm)
				}
			}
		})
	}
}

func TestCalculateNextIndexFastRollback(t *testing.T) {
	tests := []struct {
		name             string
		leaderLog        []LogEntry
		conflictTerm     int
		conflictIndex    int
		followerLogLen   int
		wantNextIndex    int
	}{
		{
			name:           "follower log too short",
			leaderLog:      []LogEntry{entry(0), entry(1), entry(2)},
			conflictTerm:   -1,
			conflictIndex:  -1,
			followerLogLen: 2,
			wantNextIndex:  2,
		},
		{
			name:           "leader has the conflicting term",
			leaderLog:      []LogEntry{entry(0), entry(1), entry(2), entry(2), entry(3)},
			conflictTerm:   2,
			conflictIndex:  2,
			followerLogLen: 5,
			wantNextIndex:  4,
		},
		{
			name:           "leader does not have the conflicting term",
			leaderLog:      []LogEntry{entry(0), entry(1), entry(3)},
			conflictTerm:   2,
			conflictIndex:  2,
			followerLogLen: 4,
			wantNextIndex:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rf := makeTestRaft(tt.leaderLog)
			got := rf.calculateNextIndexFastRollback(tt.conflictTerm, tt.conflictIndex, tt.followerLogLen)
			if got != tt.wantNextIndex {
				t.Errorf("calculateNextIndexFastRollback(%d, %d, %d) = %d, want %d",
					tt.conflictTerm, tt.conflictIndex, tt.followerLogLen, got, tt.wantNextIndex)
			}
		})
	}
}

func TestUpdateFollowerCommitIndex(t *testing.T) {
	tests := []struct {
		name                 string
		currentCommitIndex   int
		leaderCommittedIndex int
		indexBeforeNew       int
		newEntriesCount      int
		wantCommitIndex      int
	}{
		{
			name:                 "leader commit is behind",
			currentCommitIndex:   5,
			leaderCommittedIndex: 3,
			indexBeforeNew:       0,
			newEntriesCount:      10,
			wantCommitIndex:      5,
		},
		{
			name:                 "leader commit is ahead but capped by last new entry",
			currentCommitIndex:   0,
			leaderCommittedIndex: 10,
			indexBeforeNew:       2,
			newEntriesCount:      3,
			wantCommitIndex:      5,
		},
		{
			name:                 "leader commit is within new entries range",
			currentCommitIndex:   0,
			leaderCommittedIndex: 4,
			indexBeforeNew:       2,
			newEntriesCount:      5,
			wantCommitIndex:      4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rf := makeTestRaft([]LogEntry{entry(0)})
			rf.highestCommittedIndex = tt.currentCommitIndex

			rf.updateFollowerCommitIndex(tt.leaderCommittedIndex, tt.indexBeforeNew, tt.newEntriesCount)

			if rf.highestCommittedIndex != tt.wantCommitIndex {
				t.Errorf("highestCommittedIndex = %d, want %d",
					rf.highestCommittedIndex, tt.wantCommitIndex)
			}
		})
	}
}

func TestAdvanceLeaderCommitIndex(t *testing.T) {
	tests := []struct {
		name                   string
		log                    []LogEntry
		currentTerm            int
		me                     int
		peerCount              int
		highestReplicatedIndex []int
		currentCommitIndex     int
		wantCommitIndex        int
	}{
		{
			name:                   "majority replicated advances commit",
			log:                    []LogEntry{entry(0), entry(1), entry(1)},
			currentTerm:            1,
			me:                     0,
			peerCount:              3,
			highestReplicatedIndex: []int{0, 2, 2},
			currentCommitIndex:     0,
			wantCommitIndex:        2,
		},
		{
			name:                   "no majority keeps commit unchanged",
			log:                    []LogEntry{entry(0), entry(1), entry(1)},
			currentTerm:            1,
			me:                     0,
			peerCount:              3,
			highestReplicatedIndex: []int{0, 0, 0},
			currentCommitIndex:     0,
			wantCommitIndex:        0,
		},
		{
			name:                   "cannot commit entries from older terms",
			log:                    []LogEntry{entry(0), entry(1), entry(1)},
			currentTerm:            2,
			me:                     0,
			peerCount:              3,
			highestReplicatedIndex: []int{0, 2, 2},
			currentCommitIndex:     0,
			wantCommitIndex:        0,
		},
		{
			name:                   "commits highest majority-replicated index",
			log:                    []LogEntry{entry(0), entry(1), entry(2), entry(2), entry(2)},
			currentTerm:            2,
			me:                     0,
			peerCount:              5,
			highestReplicatedIndex: []int{0, 4, 3, 2, 1},
			currentCommitIndex:     0,
			wantCommitIndex:        3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rf := makeTestRaft(tt.log)
			rf.currentTerm = tt.currentTerm
			rf.me = tt.me
			rf.peers = make([]*labrpc.ClientEnd, tt.peerCount)
			rf.highestReplicatedIndex = tt.highestReplicatedIndex
			rf.highestCommittedIndex = tt.currentCommitIndex

			rf.advanceLeaderCommitIndex()

			if rf.highestCommittedIndex != tt.wantCommitIndex {
				t.Errorf("highestCommittedIndex = %d, want %d",
					rf.highestCommittedIndex, tt.wantCommitIndex)
			}
		})
	}
}

func TestAcknowledgeValidLeader(t *testing.T) {
	rf := makeTestRaft([]LogEntry{entry(0)})
	rf.role = Candidate
	rf.currentTerm = 2

	rf.acknowledgeValidLeader(3)
	if rf.role != Follower || rf.currentTerm != 3 {
		t.Errorf("Expected Follower in term 3, got %v in %d", rf.role, rf.currentTerm)
	}

	rf.role = Candidate
	rf.acknowledgeValidLeader(3)
	if rf.role != Follower || rf.currentTerm != 3 {
		t.Errorf("Expected Follower to reset heartbeat, got %v", rf.role)
	}
}

func TestTruncateLogForLocalSnapshot(t *testing.T) {
	rf := makeTestRaft([]LogEntry{entry(0), entry(1), entry(2), entry(3)})
	rf.snapshotIndex = 0

	rf.truncateLogForLocalSnapshot(2)

	if rf.snapshotIndex != 2 {
		t.Errorf("snapshotIndex = %d, want 2", rf.snapshotIndex)
	}
	if len(rf.logEntries) != 2 { // indices 2 and 3 remain (so length 2)
		t.Fatalf("len = %d, want 2", len(rf.logEntries))
	}
	if rf.logEntries[0].ElectionTerm != 2 || rf.logEntries[1].ElectionTerm != 3 {
		t.Errorf("log mismatch: %v", rf.logEntries)
	}
}

func TestTruncateLogForInstallSnapshot(t *testing.T) {
	tests := []struct {
		name              string
		log               []LogEntry
		lastIncludedIndex int
		lastIncludedTerm  int
		wantLogLength     int
		wantFirstTerm     int
	}{
		{
			name:              "has matching entry",
			log:               []LogEntry{entry(0), entry(1), entry(2), entry(3)},
			lastIncludedIndex: 2,
			lastIncludedTerm:  2,
			wantLogLength:     2,
			wantFirstTerm:     2, // term of index 2
		},
		{
			name:              "no matching entry truncates all",
			log:               []LogEntry{entry(0), entry(1), entry(2)},
			lastIncludedIndex: 4,
			lastIncludedTerm:  4,
			wantLogLength:     1,
			wantFirstTerm:     4,
		},
		{
			name:              "term mismatch truncates all",
			log:               []LogEntry{entry(0), entry(1), entry(2)},
			lastIncludedIndex: 2,
			lastIncludedTerm:  99,
			wantLogLength:     1,
			wantFirstTerm:     99,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rf := makeTestRaft(tt.log)
			rf.truncateLogForInstallSnapshot(tt.lastIncludedIndex, tt.lastIncludedTerm)

			if len(rf.logEntries) != tt.wantLogLength {
				t.Fatalf("log length = %d, want %d", len(rf.logEntries), tt.wantLogLength)
			}
			if rf.logEntries[0].ElectionTerm != tt.wantFirstTerm {
				t.Errorf("first term = %d, want %d", rf.logEntries[0].ElectionTerm, tt.wantFirstTerm)
			}
			if rf.snapshotIndex != tt.lastIncludedIndex {
				t.Errorf("snapshotIndex = %d, want %d", rf.snapshotIndex, tt.lastIncludedIndex)
			}
		})
	}
}

func TestAdvanceIndicesForSnapshot(t *testing.T) {
	rf := makeTestRaft([]LogEntry{entry(0)})
	rf.highestCommittedIndex = 1
	rf.highestAppliedIndex = 2

	rf.advanceIndicesForSnapshot(5)

	if rf.highestCommittedIndex != 5 || rf.highestAppliedIndex != 5 {
		t.Errorf("indices not advanced properly: commit=%d, apply=%d", rf.highestCommittedIndex, rf.highestAppliedIndex)
	}

	// Should not regress
	rf.advanceIndicesForSnapshot(3)
	if rf.highestCommittedIndex != 5 || rf.highestAppliedIndex != 5 {
		t.Errorf("indices regressed: commit=%d, apply=%d", rf.highestCommittedIndex, rf.highestAppliedIndex)
	}
}

func TestPushSnapshotToService(t *testing.T) {
	applyCh := make(chan raftapi.ApplyMsg, 1)
	rf := makeTestRaft([]LogEntry{entry(0)})
	rf.applyCh = applyCh
	rf.mu.Lock()

	rf.pushSnapshotToService(5, 3, []byte("data"))
	rf.mu.Unlock() // pushSnapshotToService unlocks and relocks, but we need to unlock at the end of test

	msg := <-applyCh
	if !msg.SnapshotValid || msg.SnapshotIndex != 5 || msg.SnapshotTerm != 3 || string(msg.Snapshot) != "data" {
		t.Errorf("Invalid snapshot message: %v", msg)
	}
}
