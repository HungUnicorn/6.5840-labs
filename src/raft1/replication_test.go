package raft

import (
	"testing"
)

func TestBuildAppendEntriesArgs(t *testing.T) {
	rf := makeTestRaftWithPeers([]LogEntry{entry(0), entry(1), entry(2), entry(3)}, 0, 3)
	rf.currentTerm = 2
	rf.nextLogIndexToSend[1] = 2 // Want to send entries from index 2 onwards (so indices 2 and 3)

	args := rf.buildAppendEntriesArgs(1)

	if args.Term != 2 {
		t.Errorf("args.Term = %d, want 2", args.Term)
	}
	if args.LeaderId != 0 {
		t.Errorf("args.LeaderId = %d, want 0", args.LeaderId)
	}
	if args.IndexBeforeNewEntries != 1 {
		t.Errorf("args.IndexBeforeNewEntries = %d, want 1", args.IndexBeforeNewEntries)
	}
	if args.TermBeforeNewEntries != 1 { // entry(1).ElectionTerm is 1
		t.Errorf("args.TermBeforeNewEntries = %d, want 1", args.TermBeforeNewEntries)
	}
	if len(args.NewEntries) != 2 {
		t.Fatalf("len(args.NewEntries) = %d, want 2", len(args.NewEntries))
	}
	if args.NewEntries[0].ElectionTerm != 2 || args.NewEntries[1].ElectionTerm != 3 {
		t.Errorf("args.NewEntries terms = [%d, %d], want [2, 3]", args.NewEntries[0].ElectionTerm, args.NewEntries[1].ElectionTerm)
	}
}

func TestHandleAppendEntriesReply(t *testing.T) {
	tests := []struct {
		name                 string
		serverRole           Role
		serverTerm           int
		replyTerm            int
		replySuccess         bool
		replyConflictTerm    int
		replyConflictIdx     int
		replyLogLen          int
		initialNextIdx       int
		initialMatchIdx      int
		argsNewEntriesCount  int
		argsPrevLogIdx       int
		wantNextIdx          int
		wantMatchIdx         int
		wantRole             Role
		wantTerm             int
	}{
		{
			name:                 "stale reply ignored (different term)",
			serverRole:           Leader,
			serverTerm:           3,
			replyTerm:            2, // Reply from an older RPC
			replySuccess:         true,
			initialNextIdx:       3,
			initialMatchIdx:      0,
			argsNewEntriesCount:  1,
			argsPrevLogIdx:       2,
			wantNextIdx:          3,
			wantMatchIdx:         0,
			wantRole:             Leader,
			wantTerm:             3,
		},
		{
			name:                 "reply with higher term causes step down",
			serverRole:           Leader,
			serverTerm:           3,
			replyTerm:            4,
			replySuccess:         false,
			initialNextIdx:       3,
			initialMatchIdx:      0,
			wantNextIdx:          3,
			wantMatchIdx:         0,
			wantRole:             Follower,
			wantTerm:             4,
		},
		{
			name:                 "successful reply updates indices",
			serverRole:           Leader,
			serverTerm:           3,
			replyTerm:            3,
			replySuccess:         true,
			initialNextIdx:       3,
			initialMatchIdx:      0,
			argsNewEntriesCount:  2,
			argsPrevLogIdx:       2,
			wantNextIdx:          5, // 2 + 2 + 1
			wantMatchIdx:         4, // 2 + 2
			wantRole:             Leader,
			wantTerm:             3,
		},
		{
			name:                 "delayed out-of-order success reply does not regress indices",
			serverRole:           Leader,
			serverTerm:           3,
			replyTerm:            3,
			replySuccess:         true,
			initialNextIdx:       6, // Currently ahead
			initialMatchIdx:      5, // Currently ahead
			argsNewEntriesCount:  2,
			argsPrevLogIdx:       2, // Old delayed reply reporting match at index 4
			wantNextIdx:          6, // Kept ahead
			wantMatchIdx:         5, // Kept ahead
			wantRole:             Leader,
			wantTerm:             3,
		},
		{
			name:                 "unsuccessful reply updates nextIndex via fast rollback (log too short)",
			serverRole:           Leader,
			serverTerm:           3,
			replyTerm:            3,
			replySuccess:         false,
			replyConflictTerm:    -1,
			replyConflictIdx:     -1,
			replyLogLen:          2, // Follower only has indices 0, 1
			initialNextIdx:       5,
			initialMatchIdx:      0,
			wantNextIdx:          2,
			wantMatchIdx:         0,
			wantRole:             Leader,
			wantTerm:             3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rf := makeTestRaftWithPeers([]LogEntry{entry(0), entry(1), entry(2), entry(3), entry(4)}, 0, 3)
			rf.role = tt.serverRole
			rf.currentTerm = tt.serverTerm
			rf.nextLogIndexToSend[1] = tt.initialNextIdx
			rf.highestReplicatedIndex[1] = tt.initialMatchIdx

			args := &AppendEntriesArgs{
				Term:                  tt.serverTerm, // Usually RPC was sent in same term, unless stale
				IndexBeforeNewEntries: tt.argsPrevLogIdx,
				NewEntries:            make([]LogEntry, tt.argsNewEntriesCount),
			}
			// If it's a test for stale reply from old term, override args term
			if tt.name == "stale reply ignored (different term)" {
				args.Term = tt.replyTerm
			}

			reply := &AppendEntriesReply{
				Term:          tt.replyTerm,
				Success:       tt.replySuccess,
				ConflictTerm:  tt.replyConflictTerm,
				ConflictIndex: tt.replyConflictIdx,
				LogLength:     tt.replyLogLen,
			}

			rf.handleAppendEntriesReply(1, args, reply)

			if rf.nextLogIndexToSend[1] != tt.wantNextIdx {
				t.Errorf("nextLogIndexToSend = %d, want %d", rf.nextLogIndexToSend[1], tt.wantNextIdx)
			}
			if rf.highestReplicatedIndex[1] != tt.wantMatchIdx {
				t.Errorf("highestReplicatedIndex = %d, want %d", rf.highestReplicatedIndex[1], tt.wantMatchIdx)
			}
			if rf.role != tt.wantRole {
				t.Errorf("role = %v, want %v", rf.role, tt.wantRole)
			}
			if rf.currentTerm != tt.wantTerm {
				t.Errorf("currentTerm = %d, want %d", rf.currentTerm, tt.wantTerm)
			}
		})
	}
}

func TestAppendEntriesRPC(t *testing.T) {
	tests := []struct {
		name              string
		serverLog         []LogEntry
		serverTerm        int
		argsTerm          int
		argsPrevLogIdx    int
		argsPrevLogTerm   int
		argsNewEntries    []LogEntry
		argsLeaderCommit  int
		wantSuccess       bool
		wantReplyTerm     int
		wantLogLen        int
	}{
		{
			name:            "reject stale term",
			serverLog:       []LogEntry{entry(0)},
			serverTerm:      3,
			argsTerm:        2,
			argsPrevLogIdx:  0,
			argsPrevLogTerm: 0,
			wantSuccess:     false,
			wantReplyTerm:   3,
			wantLogLen:      1,
		},
		{
			name:            "reject mismatched log",
			serverLog:       []LogEntry{entry(0), entry(1), entry(2)},
			serverTerm:      3,
			argsTerm:        3,
			argsPrevLogIdx:  2,
			argsPrevLogTerm: 99, // Mismatch
			wantSuccess:     false,
			wantReplyTerm:   3,
			wantLogLen:      3,
		},
		{
			name:            "accept valid entries and truncate conflicts",
			serverLog:       []LogEntry{entry(0), entry(1), entry(2)},
			serverTerm:      3,
			argsTerm:        3,
			argsPrevLogIdx:  1,
			argsPrevLogTerm: 1,
			argsNewEntries:  []LogEntry{entry(3)}, // Replaces index 2
			argsLeaderCommit: 2,
			wantSuccess:     true,
			wantReplyTerm:   3,
			wantLogLen:      3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rf := makeTestRaftWithPeers(tt.serverLog, 0, 3)
			rf.currentTerm = tt.serverTerm

			args := &AppendEntriesArgs{
				Term:                        tt.argsTerm,
				IndexBeforeNewEntries:       tt.argsPrevLogIdx,
				TermBeforeNewEntries:        tt.argsPrevLogTerm,
				NewEntries:                  tt.argsNewEntries,
				LeaderHighestCommittedIndex: tt.argsLeaderCommit,
			}
			reply := &AppendEntriesReply{}

			rf.AppendEntries(args, reply)

			if reply.Success != tt.wantSuccess {
				t.Errorf("Success = %v, want %v", reply.Success, tt.wantSuccess)
			}
			if reply.Term != tt.wantReplyTerm {
				t.Errorf("Reply Term = %d, want %d", reply.Term, tt.wantReplyTerm)
			}
			if len(rf.logEntries) != tt.wantLogLen {
				t.Errorf("Log length = %d, want %d", len(rf.logEntries), tt.wantLogLen)
			}
		})
	}
}
