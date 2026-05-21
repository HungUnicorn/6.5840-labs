package raft

import (
	"math/rand"
	"time"
)

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	candidateIsUpToDate := rf.isCandidateLogUpToDate(args.LastLogTerm, args.LastLogIndex)
	canVoteForCandidate := rf.votedFor == -1 || rf.votedFor == args.CandidateId

	if canVoteForCandidate && candidateIsUpToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.lastHeartbeat = time.Now()
	} else {
		reply.VoteGranted = false
	}

	reply.Term = rf.currentTerm
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) startElection(term int) {
	rf.mu.Lock()
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLatestLogIndex(),
		LastLogTerm:  rf.getLatestLogTerm(),
	}
	rf.mu.Unlock()

	votesReceived := 1

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(peer int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(peer, &args, &reply)

			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				isStaleReply := rf.currentTerm != args.Term || rf.role != Candidate
				if isStaleReply {
					return
				}

				if reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term)
					return
				}

				if reply.VoteGranted {
					votesReceived++

					hasMajority := votesReceived > len(rf.peers)/2
					if hasMajority {
						rf.becomeLeader()
					}
				}
			}
		}(i)
	}
}

func (rf *Raft) becomeFollower(newTerm int) {
	rf.role = Follower
	rf.currentTerm = newTerm
	rf.votedFor = -1
	rf.lastHeartbeat = time.Now()
}

func (rf *Raft) becomeCandidate() {
	rf.role = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastHeartbeat = time.Now()
}

func (rf *Raft) becomeLeader() {
	rf.role = Leader

	totalPeers := len(rf.peers)
	rf.nextLogIndexToSend = make([]int, totalPeers)
	rf.highestReplicatedIndex = make([]int, totalPeers)

	latestIndex := rf.getLatestLogIndex()
	for peerId := range rf.peers {
		rf.nextLogIndexToSend[peerId] = latestIndex + 1
		rf.highestReplicatedIndex[peerId] = 0
	}

	go rf.heartbeatTicker()
}

func (rf *Raft) ticker() {
	for true {
		ms := ElectionTimeoutBase + (rand.Int63() % ElectionTimeoutRange)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		rf.mu.Lock()
		if rf.role != Leader && time.Since(rf.lastHeartbeat) > ElectionTimeoutBase*time.Millisecond {
			rf.becomeCandidate()
			go rf.startElection(rf.currentTerm)
		}
		rf.mu.Unlock()
	}
}
