package raft

import (
	"time"

	"6.5840/raftapi"
)

func (rf *Raft) getLatestLogIndex() int {
	return len(rf.logEntries) - 1
}

func (rf *Raft) getLatestLogTerm() int {
	latestIndex := rf.getLatestLogIndex()
	return rf.logEntries[latestIndex].ElectionTerm
}

func (rf *Raft) advanceLeaderCommitIndex() {
	// Start looking from the newest entry backwards down to the current commit index
	for indexToCheck := rf.getLatestLogIndex(); indexToCheck > rf.highestCommittedIndex; indexToCheck-- {

		// Raft Safety Rule (Section 5.4.2): A leader can only safely commit entries
		// from its *current* term by counting replicas.
		if rf.logEntries[indexToCheck].ElectionTerm != rf.currentTerm {
			continue
		}

		replicaCount := 1 // The leader always has a copy of its own log
		for peerId := range rf.peers {
			if peerId == rf.me {
				continue
			}
			if rf.highestReplicatedIndex[peerId] >= indexToCheck {
				replicaCount++
			}
		}

		hasMajority := replicaCount > len(rf.peers)/2
		if hasMajority {
			rf.highestCommittedIndex = indexToCheck
			rf.applyCommittedEntries()
			break // We found the highest valid index, no need to check lower ones
		}
	}
}

func (rf *Raft) applyCommittedEntries() {
	// Send all newly committed entries to the tester
	for rf.highestCommittedIndex > rf.highestAppliedIndex {
		rf.highestAppliedIndex++

		message := raftapi.ApplyMsg{
			CommandValid: true,
			Command:      rf.logEntries[rf.highestAppliedIndex].Command,
			CommandIndex: rf.highestAppliedIndex,
		}

		// CRITICAL: Unlock before sending to the channel to avoid deadlocks!
		// The tester might block while reading from the channel.
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

func (rf *Raft) checkLogMatching(args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	reply.ConflictTerm = -1
	reply.ConflictIndex = -1
	reply.LogLength = len(rf.logEntries)

	// Case 3: Follower's log is too short
	if rf.getLatestLogIndex() < args.IndexBeforeNewEntries {
		return false
	}

	// Cases 1 & 2: Term mismatch at the specified index
	if rf.logEntries[args.IndexBeforeNewEntries].ElectionTerm != args.TermBeforeNewEntries {
		reply.ConflictTerm = rf.logEntries[args.IndexBeforeNewEntries].ElectionTerm

		// Find the very first index of this conflicting term to skip the whole term at once
		firstIndexOfConflictingTerm := args.IndexBeforeNewEntries
		for firstIndexOfConflictingTerm > 1 && rf.logEntries[firstIndexOfConflictingTerm-1].ElectionTerm == reply.ConflictTerm {
			firstIndexOfConflictingTerm--
		}

		reply.ConflictIndex = firstIndexOfConflictingTerm
		return false
	}

	return true
}

func (rf *Raft) mergeLogEntries(indexBeforeNew int, newEntries []LogEntry) {
	for i, incomingEntry := range newEntries {
		targetIndex := indexBeforeNew + 1 + i

		// If we have an entry at this target index, check for a term conflict
		if targetIndex <= rf.getLatestLogIndex() {
			if rf.logEntries[targetIndex].ElectionTerm != incomingEntry.ElectionTerm {
				// Conflict! Truncate our log right before the conflict and append the rest
				rf.logEntries = rf.logEntries[:targetIndex]
				rf.logEntries = append(rf.logEntries, newEntries[i:]...)
				return // We appended everything remaining, so we are done
			}
		} else {
			// No existing entry here; safely append all remaining incoming entries
			rf.logEntries = append(rf.logEntries, newEntries[i:]...)
			return
		}
	}
}

func (rf *Raft) updateFollowerCommitIndex(leaderCommittedIndex int, indexBeforeNew int, newEntriesCount int) {
	if leaderCommittedIndex <= rf.highestCommittedIndex {
		return
	}

	indexOfLastNewEntry := indexBeforeNew + newEntriesCount

	// Commit index becomes the minimum of the leader's commit index and our latest entry
	if leaderCommittedIndex < indexOfLastNewEntry {
		rf.highestCommittedIndex = leaderCommittedIndex
	} else {
		rf.highestCommittedIndex = indexOfLastNewEntry
	}

	// Push newly committed entries to the state machine
	rf.applyCommittedEntries()
}

func (rf *Raft) isCandidateLogUpToDate(candidateLastLogTerm int, candidateLastLogIndex int) bool {
	myLastLogTerm := rf.getLatestLogTerm()
	myLastLogIndex := rf.getLatestLogIndex()

	// Rule 1: If the terms differ, the higher term wins
	if candidateLastLogTerm != myLastLogTerm {
		return candidateLastLogTerm > myLastLogTerm
	}

	// Rule 2: If the terms are the same, the longer log wins
	return candidateLastLogIndex >= myLastLogIndex
}

func (rf *Raft) calculateNextIndexFastRollback(conflictTerm int, conflictIndex int, logLength int) int {
	// Case 3: Follower log was completely missing the requested index
	if conflictTerm == -1 {
		return logLength
	}

	// Search our own log backwards to see if we have the follower's conflicting term
	for i := rf.getLatestLogIndex(); i > 0; i-- {
		if rf.logEntries[i].ElectionTerm == conflictTerm {
			// Case 2: We HAVE the term. The follower should keep everything up to this point.
			return i + 1
		}
	}

	// Case 1: We DO NOT have the term. Force the follower to wipe out that entire term.
	return conflictIndex
}
