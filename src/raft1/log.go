package raft

import (
	"time"

	"6.5840/raftapi"
)

func (rf *Raft) getLatestLogIndex() int {
	return len(rf.logEntries) - 1
}

func (rf *Raft) getLatestLogTerm() int {
	return rf.logEntries[rf.getLatestLogIndex()].ElectionTerm
}

func (rf *Raft) advanceLeaderCommitIndex() {
	for candidateIndex := rf.getLatestLogIndex(); candidateIndex > rf.highestCommittedIndex; candidateIndex-- {
		isNotFromCurrentTerm := rf.logEntries[candidateIndex].ElectionTerm != rf.currentTerm
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
		message := raftapi.ApplyMsg{
			CommandValid: true,
			Command:      rf.logEntries[rf.highestAppliedIndex].Command,
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
	reply.LogLength = len(rf.logEntries)

	logIsTooShort := rf.getLatestLogIndex() < args.IndexBeforeNewEntries
	if logIsTooShort {
		return false
	}

	termAtPrevIndex := rf.logEntries[args.IndexBeforeNewEntries].ElectionTerm
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
	for firstIndex > 1 && rf.logEntries[firstIndex-1].ElectionTerm == term {
		firstIndex--
	}
	return firstIndex
}

func (rf *Raft) mergeLogEntries(indexBeforeNew int, newEntries []LogEntry) {
	for i, incomingEntry := range newEntries {
		targetIndex := indexBeforeNew + 1 + i

		existsLocally := targetIndex <= rf.getLatestLogIndex()
		if existsLocally {
			hasConflict := rf.logEntries[targetIndex].ElectionTerm != incomingEntry.ElectionTerm
			if hasConflict {
				rf.logEntries = rf.logEntries[:targetIndex]
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
	myLastLogIndex := rf.getLatestLogIndex()

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

	for i := rf.getLatestLogIndex(); i > 0; i-- {
		leaderHasConflictTerm := rf.logEntries[i].ElectionTerm == conflictTerm
		if leaderHasConflictTerm {
			return i + 1
		}
	}

	return conflictIndex
}
