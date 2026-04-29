package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Persistent state
	currentTerm int
	votedFor    int

	// Volatile state
	role          Role
	lastHeartbeat time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term        int
	CandidateId int
	// Your data here (3A, 3B).
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
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

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.lastHeartbeat = time.Now()
	} else {
		reply.VoteGranted = false
	}

	reply.Term = rf.currentTerm
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
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

	// Your initialization code here (3A, 3B, 3C).
	rf.mu.Lock()
	rf.role = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.lastHeartbeat = time.Now()
	rf.mu.Unlock()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) startElection(term int) {
	args := RequestVoteArgs{
		Term:        term,
		CandidateId: rf.me,
	}

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

// becomeFollower transitions the server to a Follower and updates its term.
// Must be called with rf.mu held.
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
	go rf.heartbeatTicker()
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}

	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(peer int) {
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(peer, &args, &reply)

			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.currentTerm != args.Term || rf.role != Leader {
					return
				}

				if reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term)
					return
				}
			}
		}(i)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	} else {
		// Same term: just acknowledge the leader and reset timer
		rf.role = Follower
		rf.lastHeartbeat = time.Now()
	}
	reply.Term = rf.currentTerm
	reply.Success = true
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
