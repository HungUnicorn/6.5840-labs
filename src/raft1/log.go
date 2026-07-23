package raft

import (
	"time"

	"6.5840/raftapi"
)

func (rf *Raft) logical2Physical(logicalIndex int) int {
	return logicalIndex - rf.snapshotIndex
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.logEntries) - 1 + rf.snapshotIndex
}

func (rf *Raft) getLatestLogTerm() int {
	return rf.logEntries[rf.logical2Physical(rf.getLastLogIndex())].ElectionTerm
}

func (rf *Raft) advanceLeaderCommitIndex() {
	for candidateIndex := rf.getLastLogIndex(); candidateIndex > rf.highestCommittedIndex; candidateIndex-- {
		isBeforeSnapshot := candidateIndex <= rf.snapshotIndex
		if isBeforeSnapshot {
			break
		}

		isNotFromCurrentTerm := rf.logEntries[rf.logical2Physical(candidateIndex)].ElectionTerm != rf.currentTerm
		if isNotFromCurrentTerm {
			continue
		}

		replicaCount := 1
		for peerId := range rf.peers {
			if peerId != rf.me && rf.highestReplicatedIndex[peerId] >= candidateIndex {
				replicaCount++
			}
		}

		if replicaCount > len(rf.peers)/2 {
			rf.highestCommittedIndex = candidateIndex
			rf.applyCond.Signal()
			break
		}
	}
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for {
		for rf.highestAppliedIndex >= rf.highestCommittedIndex {
			rf.applyCond.Wait()
		}

		rf.highestAppliedIndex++
		
		isWokenUpAfterInstallSnapshot := rf.highestAppliedIndex <= rf.snapshotIndex
		if isWokenUpAfterInstallSnapshot {
			rf.highestAppliedIndex = rf.snapshotIndex
			continue
		}

		message := raftapi.ApplyMsg{
			CommandValid: true,
			Command:      rf.logEntries[rf.logical2Physical(rf.highestAppliedIndex)].Command,
			CommandIndex: rf.highestAppliedIndex,
		}

		rf.mu.Unlock()
		rf.applyCh <- message
		rf.mu.Lock()
	}
}

func (rf *Raft) acknowledgeValidLeader(leaderTerm int) {
	if leaderTerm > rf.currentTerm {
		rf.becomeFollower(leaderTerm)
	} else {
		rf.role = Follower
		rf.lastHeartbeat = time.Now()
	}
}

func (rf *Raft) checkLogConsistency(args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	reply.ConflictTerm = -1
	reply.ConflictIndex = -1
	reply.LogLength = len(rf.logEntries) + rf.snapshotIndex

	logIsTooShort := rf.getLastLogIndex() < args.IndexBeforeNewEntries
	if logIsTooShort {
		return false
	}

	leaderPrevIndexDiscardedBySnapshot := args.IndexBeforeNewEntries < rf.snapshotIndex
	if leaderPrevIndexDiscardedBySnapshot {
		return false
	}

	termAtPrevIndex := rf.logEntries[rf.logical2Physical(args.IndexBeforeNewEntries)].ElectionTerm
	termsMatch := termAtPrevIndex == args.TermBeforeNewEntries
	if !termsMatch {
		reply.ConflictTerm = termAtPrevIndex
		reply.ConflictIndex = rf.findFirstIndexOfTerm(termAtPrevIndex, args.IndexBeforeNewEntries)
		return false
	}

	return true
}

func (rf *Raft) findFirstIndexOfTerm(term int, startIndex int) int {
	firstIndex := startIndex
	for firstIndex > rf.snapshotIndex+1 && rf.logEntries[rf.logical2Physical(firstIndex-1)].ElectionTerm == term {
		firstIndex--
	}
	return firstIndex
}

func (rf *Raft) mergeLogEntries(indexBeforeNew int, newEntries []LogEntry) {
	for i, incomingEntry := range newEntries {
		targetIndex := indexBeforeNew + 1 + i

		isAlreadySnapshotted := targetIndex <= rf.snapshotIndex
		if isAlreadySnapshotted {
			continue
		}

		existsLocally := targetIndex <= rf.getLastLogIndex()
		if existsLocally {
			physicalIndex := rf.logical2Physical(targetIndex)
			hasConflict := rf.logEntries[physicalIndex].ElectionTerm != incomingEntry.ElectionTerm
			if hasConflict {
				rf.logEntries = rf.logEntries[:physicalIndex]
				rf.logEntries = append(rf.logEntries, newEntries[i:]...)
				rf.persist()
				return
			}
		} else {
			rf.logEntries = append(rf.logEntries, newEntries[i:]...)
			rf.persist()
			return
		}
	}
}

func (rf *Raft) updateFollowerCommitIndex(leaderCommittedIndex int, indexBeforeNew int, newEntriesCount int) {
	if leaderCommittedIndex <= rf.highestCommittedIndex {
		return
	}

	indexOfLastNewEntry := indexBeforeNew + newEntriesCount
	if leaderCommittedIndex < indexOfLastNewEntry {
		rf.highestCommittedIndex = leaderCommittedIndex
	} else {
		rf.highestCommittedIndex = indexOfLastNewEntry
	}

	rf.applyCond.Signal()
}

func (rf *Raft) isCandidateLogUpToDate(candidateLastLogTerm int, candidateLastLogIndex int) bool {
	myLastLogTerm := rf.getLatestLogTerm()
	myLastLogIndex := rf.getLastLogIndex()

	if candidateLastLogTerm != myLastLogTerm {
		return candidateLastLogTerm > myLastLogTerm
	}
	return candidateLastLogIndex >= myLastLogIndex
}

func (rf *Raft) calculateNextIndexFastRollback(conflictTerm int, conflictIndex int, followerLogLength int) int {
	followerLogWasTooShort := conflictTerm == -1
	if followerLogWasTooShort {
		return followerLogLength
	}

	for i := rf.getLastLogIndex(); i > rf.snapshotIndex; i-- {
		leaderHasConflictTerm := rf.logEntries[rf.logical2Physical(i)].ElectionTerm == conflictTerm
		if leaderHasConflictTerm {
			return i + 1
		}
	}

	return conflictIndex
}

func (rf *Raft) truncateLogForLocalSnapshot(index int) {
	physicalIndex := rf.logical2Physical(index)
	newLog := make([]LogEntry, len(rf.logEntries)-physicalIndex)
	copy(newLog, rf.logEntries[physicalIndex:])
	rf.logEntries = newLog
	rf.snapshotIndex = index
}

func (rf *Raft) truncateLogForInstallSnapshot(lastIncludedIndex int, lastIncludedTerm int) {
	hasMatchingEntry := lastIncludedIndex <= rf.getLastLogIndex() &&
		rf.logEntries[rf.logical2Physical(lastIncludedIndex)].ElectionTerm == lastIncludedTerm

	if hasMatchingEntry {
		physicalIndex := rf.logical2Physical(lastIncludedIndex)
		newLog := make([]LogEntry, len(rf.logEntries)-physicalIndex)
		copy(newLog, rf.logEntries[physicalIndex:])
		rf.logEntries = newLog
	} else {
		rf.logEntries = []LogEntry{{ElectionTerm: lastIncludedTerm, Command: nil}}
	}

	rf.snapshotIndex = lastIncludedIndex
}

func (rf *Raft) advanceIndicesForSnapshot(lastIncludedIndex int) {
	if rf.highestCommittedIndex < lastIncludedIndex {
		rf.highestCommittedIndex = lastIncludedIndex
	}
	if rf.highestAppliedIndex < lastIncludedIndex {
		rf.highestAppliedIndex = lastIncludedIndex
	}
}

func (rf *Raft) pushSnapshotToService(lastIncludedIndex int, lastIncludedTerm int, data []byte) {
	msg := raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      data,
		SnapshotTerm:  lastIncludedTerm,
		SnapshotIndex: lastIncludedIndex,
	}

	rf.mu.Unlock()
	rf.applyCh <- msg
	rf.mu.Lock()
}
