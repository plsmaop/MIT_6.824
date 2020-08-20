package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// Log structure for Raft
//
type entry struct {
	CommandIndex int
	Term         int
	Commited     bool
	Command      interface{}
}

//
// State for Raft
//
type state int

const (
	follower state = iota
	candidate
	leader
)

const (
	electionTimeoutPeriodBase = int64(time.Millisecond * 1000)
	randMax                   = 1000
	randMin                   = 100
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	logs        []entry
	state       state

	// volatile state
	commitIndex int
	lastApplied int

	// on leader
	nextIndex  []int
	matchIndex []int

	// for election
	receivedVote          int
	electionTimeout       int64
	electionTimeoutPeriod int64

	// handle append entry
	appendChan chan appendEntriesTaskArgs
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

// must be used in critical section
func (rf *Raft) updateTerm(term int) {
	// update term
	if term <= rf.currentTerm {
		return
	}

	rf.currentTerm = term
	rf.state = follower
	rf.votedFor = -1
	rf.receivedVote = 0
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// check log up-to-date for received vote
// must be used in critical section
//
func (rf *Raft) checkLogUpTodate(args *RequestVoteArgs) bool {
	// start from 1
	logLastIndex := len(rf.logs)
	if len(rf.logs) == 0 {
		return true
	}

	if args.LastLogTerm > rf.logs[logLastIndex-1].Term {
		// last log with latest term
		return true
	} else if args.LastLogTerm == rf.logs[logLastIndex-1].Term {
		// same last term but longer log
		return args.LastLogIndex >= logLastIndex
	}

	return false
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.killed() {
		return
	}
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.updateTerm(args.Term)

	grant := (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && args.Term == rf.currentTerm && rf.checkLogUpTodate(args)
	if grant {
		// first one wins
		rf.votedFor = args.CandidateID
		rf.electionTimeout = time.Now().UnixNano() + int64(rf.electionTimeoutPeriod)
	}

	reply.VoteGranted = grant
	reply.Term = rf.currentTerm
	DPrintf("%d vote for %d in term %d", rf.me, rf.votedFor, rf.currentTerm)
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	if rf.killed() {
		return false
	}
	DPrintf("%d request vote to %d for term: %d", rf.me, server, args.Term)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// must be used in critical section
func (rf *Raft) becomeLeader() {
	if rf.state != candidate {
		return
	}

	rf.state = leader
	for peerInd := range rf.nextIndex {
		nextInd := len(rf.logs) + 1
		rf.nextIndex[peerInd] = nextInd
		rf.matchIndex[peerInd] = 0
	}

	// new term enrty
	rf.logs = append(rf.logs, entry{
		CommandIndex: len(rf.logs) + 1,
		Term:         rf.currentTerm,
		Commited:     false,
		Command:      nil,
	})

	DPrintf("%d become leader", rf.me)
}

type appendEntriesTaskArgs struct {
	peerInd int
	nextInd int
	args    AppendEntriesArgs
}

type electionArgs struct {
	peerInd int
	args    RequestVoteArgs
}

// helper function
func (rf *Raft) getRequestVoteArgs(now time.Time) (bool, RequestVoteArgs) {
	if rf.killed() {
		return false, RequestVoteArgs{}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == leader || !now.After(time.Unix(0, rf.electionTimeout)) {
		return false, RequestVoteArgs{}
	}

	// reset timeout
	rf.electionTimeout = now.UnixNano() + rf.electionTimeoutPeriod

	// start new election
	rf.receivedVote = 1
	rf.votedFor = rf.me
	rf.state = candidate
	rf.currentTerm++
	lastLogIndex := len(rf.logs)
	lastLogTerm := 0
	if len(rf.logs) > 0 {
		lastLogTerm = rf.logs[lastLogIndex-1].Term
	}

	return true, RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
}

func (rf *Raft) getAppendEntriesTaskArgs(now time.Time) (bool, []appendEntriesTaskArgs) {
	if rf.killed() {
		return false, nil
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != leader || !now.After(time.Unix(0, rf.electionTimeout-((1*rf.electionTimeoutPeriod)/10))) {
		return false, nil
	}

	// reset timeout
	if now.After(time.Unix(0, rf.electionTimeout)) {
		rf.electionTimeout = now.UnixNano() + rf.electionTimeoutPeriod
	}

	appendEntriesTaskArgsToSend := []appendEntriesTaskArgs{}
	for ind := range rf.peers {
		if ind == rf.me {
			continue
		}

		nextInd := rf.nextIndex[ind]
		prevLogTerm := rf.getPrevLogTerm(nextInd)
		appendEntriesTaskArgsToSend = append(appendEntriesTaskArgsToSend, appendEntriesTaskArgs{
			peerInd: ind,
			nextInd: rf.nextIndex[ind],
			args: AppendEntriesArgs{
				Term:              rf.currentTerm,
				LeaderID:          rf.me,
				LeaderCommitIndex: rf.commitIndex,
				Entries:           []entry{},
				PrevLogIndex:      nextInd - 1,
				PrevLogTerm:       prevLogTerm,
			},
		})
	}

	return true, appendEntriesTaskArgsToSend
}

func (rf *Raft) loop(ctx context.Context) {
	electionChan := make(chan electionArgs, len(rf.peers))
	go func() {
		for {
			now := time.Now()
			shouldElect, requestVoteArgs := rf.getRequestVoteArgs(now)
			shouldSendHeartbeat, appendEntriesTaskArgsToSend := rf.getAppendEntriesTaskArgs(now)
			if shouldElect {
				for ind := range rf.peers {
					if ind == rf.me {
						continue
					}

					electionChan <- electionArgs{
						peerInd: ind,
						args:    requestVoteArgs,
					}
				}
			}

			if shouldSendHeartbeat {
				for _, appendEntriesTaskArgs := range appendEntriesTaskArgsToSend {
					rf.appendChan <- appendEntriesTaskArgs
				}
			}
			time.Sleep(time.Millisecond)
		}
	}()

	for range rf.peers {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case electionArgs := <-electionChan:
					rf.startElection(electionArgs.peerInd, electionArgs.args)
				case appendEntriesTaskArgs := <-rf.appendChan:
					rf.startAppendEntries(appendEntriesTaskArgs.peerInd, appendEntriesTaskArgs.nextInd, appendEntriesTaskArgs.args)
				}
			}
		}()
	}
}

func (rf *Raft) startElection(peerInd int, args RequestVoteArgs) {
	if rf.killed() {
		return
	}

	reply := RequestVoteReply{}
	if !rf.sendRequestVote(peerInd, &args, &reply) {
		return
	}
	rf.handleRequestVoteResponse(peerInd, &reply)
}

func (rf *Raft) handleRequestVoteResponse(peerInd int, reply *RequestVoteReply) {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	rf.updateTerm(reply.Term)

	if rf.currentTerm > reply.Term || rf.state == follower {
		// drop stale response
		DPrintf("%d Drop response from %d in term %d", rf.me, peerInd, reply.Term)
		rf.mu.Unlock()
		return
	}

	if rf.receivedVote > len(rf.peers)/2 || !reply.VoteGranted {
		DPrintf("%d received from %d %v", rf.me, peerInd, reply)
		rf.mu.Unlock()
		return
	}

	rf.receivedVote++
	DPrintf("%d received vote from %d in term %d", rf.me, peerInd, reply.Term)
	DPrintf("%d received vote number: %d", rf.me, rf.receivedVote)
	if !(rf.receivedVote > len(rf.peers)/2) {
		rf.mu.Unlock()
		return
	}

	// become leader
	rf.becomeLeader()
	appendEntriesTaskArgsToSend := []appendEntriesTaskArgs{}
	for ind := range rf.peers {
		if ind == rf.me {
			continue
		}

		nextInd := rf.nextIndex[ind]
		prevLogTerm := rf.getPrevLogTerm(nextInd)
		appendEntriesTaskArgsToSend = append(appendEntriesTaskArgsToSend, appendEntriesTaskArgs{
			peerInd: ind,
			nextInd: nextInd,
			args: AppendEntriesArgs{
				Term:              rf.currentTerm,
				LeaderID:          rf.me,
				PrevLogIndex:      nextInd - 1,
				PrevLogTerm:       prevLogTerm,
				Entries:           rf.logs[nextInd-1:],
				LeaderCommitIndex: rf.commitIndex,
			},
		})
	}
	rf.mu.Unlock()

	for _, appendEntriesTaskArgs := range appendEntriesTaskArgsToSend {
		rf.appendChan <- appendEntriesTaskArgs
	}
}

type AppendEntriesArgs struct {
	Term              int
	LeaderID          int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []entry
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) startAppendEntries(peerInd, nextInd int, args AppendEntriesArgs) {
	if rf.killed() {
		return
	}

	reply := AppendEntriesReply{}

	if !rf.sendAppendEntries(peerInd, &args, &reply) {
		return
	}

	rf.handleAppendEntriesResponse(peerInd, &args, &reply)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if rf.killed() {
		return false
	}

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.updateTerm(args.Term)
	termMatch := rf.checkLogTermMatch(args)

	reply.Term = rf.currentTerm

	// reject
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.state == follower && !termMatch) {
		reply.Success = false
		DPrintf("%d fuck", rf.me)
		return
	}

	// return to follower
	if args.Term == rf.currentTerm && rf.state == candidate {
		rf.state = follower
	}

	// reset timeout
	rf.electionTimeout = time.Now().UnixNano() + rf.electionTimeoutPeriod

	// accept
	if args.Term == rf.currentTerm && rf.state == follower && termMatch {
		reply.Success = true
		nextIndex := args.PrevLogIndex + 1
		if len(args.Entries) == 0 || (len(rf.logs) >= nextIndex && rf.logs[nextIndex-1].Term == args.Entries[0].Term) {
			// processed request
			rf.commitIndex = args.LeaderCommitIndex
		} else if len(args.Entries) > 0 {
			if len(rf.logs) >= nextIndex && rf.logs[nextIndex-1].Term != args.Entries[0].Term {
				// conflict, drop conflicted logs
				rf.logs = rf.logs[:nextIndex-1]
			}

			if len(rf.logs) == args.PrevLogIndex {
				// append
				rf.logs = append(rf.logs, args.Entries...)
			}
		}
	}

	// update commit index
	if args.LeaderCommitIndex > rf.commitIndex {
		if args.LeaderCommitIndex < len(rf.logs) {
			rf.commitIndex = args.LeaderCommitIndex
		} else {
			rf.commitIndex = len(rf.logs)
		}
	}
}

// must be used in critical section
func (rf *Raft) checkLogTermMatch(args *AppendEntriesArgs) bool {
	return args.PrevLogIndex == 0 || (args.PrevLogIndex <= len(rf.logs) && args.PrevLogTerm == rf.logs[args.PrevLogIndex-1].Term)
}

func (rf *Raft) handleAppendEntriesResponse(peerInd int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	rf.updateTerm(reply.Term)

	if rf.currentTerm > reply.Term || rf.state != leader {
		// drop stale response
		rf.mu.Unlock()
		return
	}

	shouldRetry := false
	nextInd := 0
	appendEntriesTaskArgsToSend := appendEntriesTaskArgs{}
	if reply.Success {
		matchIndex := args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peerInd] = matchIndex + 1
		rf.matchIndex[peerInd] = matchIndex
	} else {
		nextIndex := rf.nextIndex[peerInd] - 1
		if nextIndex < 1 {
			rf.nextIndex[peerInd] = 1
		} else {
			rf.nextIndex[peerInd] = nextIndex
		}

		// retry
		shouldRetry = true
		nextInd = rf.nextIndex[peerInd]
		prevLogTerm := rf.getPrevLogTerm(nextInd)
		appendEntriesTaskArgsToSend = appendEntriesTaskArgs{
			peerInd: peerInd,
			nextInd: nextInd,
			args: AppendEntriesArgs{
				Term:              rf.currentTerm,
				LeaderID:          rf.me,
				PrevLogIndex:      nextInd - 1,
				PrevLogTerm:       prevLogTerm,
				Entries:           rf.logs[nextInd-1:],
				LeaderCommitIndex: rf.commitIndex,
			},
		}
	}
	rf.mu.Unlock()

	if shouldRetry {
		rf.appendChan <- appendEntriesTaskArgsToSend
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) handleIsKilled(cancel context.CancelFunc) {
	go func() {
		defer cancel()
		for !rf.killed() {
		}
	}()
}

// helper function
// must be used in critical section
func (rf *Raft) getPrevLogTerm(nextInd int) int {
	prevLogTerm := 0
	if nextInd > 1 {
		prevLogTerm = rf.logs[nextInd-2].Term
	}

	return prevLogTerm
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	timeout := electionTimeoutPeriodBase + (rand.Int63n(randMax-randMin)+randMin)*int64(time.Millisecond)
	rf := &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		dead:        0,
		currentTerm: 0,
		votedFor:    -1,
		logs:        []entry{},
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		state:       follower,

		receivedVote:          0,
		electionTimeout:       time.Now().UnixNano() + timeout,
		electionTimeoutPeriod: timeout,

		appendChan: make(chan appendEntriesTaskArgs, len(peers)),
	}
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	ctx, cancel := context.WithCancel(context.Background())

	rf.loop(ctx)
	rf.handleIsKilled(cancel)

	return rf
}
