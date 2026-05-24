package raft

import (
	"time"
)

type AppendEntriesArgs struct {
	Term                        int
	LeaderId                    int
	IndexBeforeNewEntries       int
	TermBeforeNewEntries        int
	NewEntries                  []LogEntry
	LeaderHighestCommittedIndex int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictTerm  int
	ConflictIndex int
	LogLength     int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) heartbeatTicker() {
	for true {
		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		rf.broadcastAppendEntries()
		time.Sleep(HeartbeatInterval)
	}
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return
	}

	for peerId := range rf.peers {
		if peerId == rf.me {
			continue
		}

		go rf.replicateToPeer(peerId, rf.currentTerm)
	}
}

func (rf *Raft) replicateToPeer(targetPeerId int, termSnapshot int) {
	rf.mu.Lock()
	if rf.role != Leader || rf.currentTerm != termSnapshot {
		rf.mu.Unlock()
		return
	}

	if rf.nextLogIndexToSend[targetPeerId] <= rf.snapshotIndex {
		args := InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.snapshotIndex,
			LastIncludedTerm:  rf.logEntries[0].ElectionTerm,
			Data:              rf.persister.ReadSnapshot(),
		}
		rf.mu.Unlock()

		reply := InstallSnapshotReply{}
		if rf.sendInstallSnapshot(targetPeerId, &args, &reply) {
			rf.handleInstallSnapshotReply(targetPeerId, &args, &reply)
		}
		return
	}

	args := rf.buildAppendEntriesArgs(targetPeerId)
	rf.mu.Unlock()

	reply := AppendEntriesReply{}
	rpcSuccessful := rf.sendAppendEntries(targetPeerId, &args, &reply)

	if rpcSuccessful {
		rf.handleAppendEntriesReply(targetPeerId, &args, &reply)
	}
}

func (rf *Raft) handleInstallSnapshotReply(targetPeerId int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm != args.Term || rf.role != Leader {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}

	if args.LastIncludedIndex > rf.highestReplicatedIndex[targetPeerId] {
		rf.highestReplicatedIndex[targetPeerId] = args.LastIncludedIndex
		rf.nextLogIndexToSend[targetPeerId] = args.LastIncludedIndex + 1
		rf.advanceLeaderCommitIndex()
	}
}

func (rf *Raft) buildAppendEntriesArgs(targetPeerId int) AppendEntriesArgs {
	nextIndexRequired := rf.nextLogIndexToSend[targetPeerId]
	indexBeforeNew := nextIndexRequired - 1
	termBeforeNew := rf.logEntries[rf.logical2Physical(indexBeforeNew)].ElectionTerm

	entriesToSend := make([]LogEntry, rf.getLatestLogIndex()-indexBeforeNew)
	copy(entriesToSend, rf.logEntries[rf.logical2Physical(nextIndexRequired):])

	return AppendEntriesArgs{
		Term:                        rf.currentTerm,
		LeaderId:                    rf.me,
		IndexBeforeNewEntries:       indexBeforeNew,
		TermBeforeNewEntries:        termBeforeNew,
		NewEntries:                  entriesToSend,
		LeaderHighestCommittedIndex: rf.highestCommittedIndex,
	}
}

func (rf *Raft) handleAppendEntriesReply(targetPeerId int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isStaleReply := rf.currentTerm != args.Term || rf.role != Leader
	if isStaleReply {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}

	if reply.Success {
		// Safety check to prevent out-of-order delayed replies from regressing the match index
		matchIdx := args.IndexBeforeNewEntries + len(args.NewEntries)
		if matchIdx > rf.highestReplicatedIndex[targetPeerId] {
			rf.highestReplicatedIndex[targetPeerId] = matchIdx
			rf.nextLogIndexToSend[targetPeerId] = matchIdx + 1
			rf.advanceLeaderCommitIndex()
		}
	} else {
		rf.nextLogIndexToSend[targetPeerId] = rf.calculateNextIndexFastRollback(
			reply.ConflictTerm,
			reply.ConflictIndex,
			reply.LogLength,
		)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	rf.acknowledgeValidLeader(args.Term)
	reply.Term = rf.currentTerm

	if !rf.checkLogConsistency(args, reply) {
		reply.Success = false
		return
	}

	rf.mergeLogEntries(args.IndexBeforeNewEntries, args.NewEntries)

	rf.updateFollowerCommitIndex(args.LeaderHighestCommittedIndex, args.IndexBeforeNewEntries, len(args.NewEntries))

	reply.Success = true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	rf.acknowledgeValidLeader(args.Term)
	reply.Term = rf.currentTerm

	if args.LastIncludedIndex <= rf.snapshotIndex {
		return
	}

	rf.commitSnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Data)
}
