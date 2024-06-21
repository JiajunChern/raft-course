package raft

import (
	"fmt"
	"math/rand"
	"time"
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (PartA, PartB).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (PartA).
	Term        int
	VoteGranted bool // 要票结果
}

func (rf *Raft) resetElectionTimerLocked() {
	rf.electionStart = time.Now()
	randRange := int64(electionTimeoutMax - electionTimeoutMin)
	rf.electionTimeout = electionTimeoutMin + time.Duration(rand.Int63()%randRange) // 设置随机的超时时间
}

func (rf *Raft) isElectionTimeoutLocked() bool {
	return time.Since(rf.electionStart) > rf.electionTimeout
}

func (rf *Raft) isMoreUpToDateLocked(candidateIndex, candidateTerm int) bool {
	l := len(rf.log)
	lastTerm, lastIndex := rf.log[l-1].Term, l-1
	LOG(rf.me, rf.currentTerm, DVote, "Compare last log, Me: [%d]T%d, Candidate: [%d]T%d", lastIndex, lastTerm, candidateIndex, candidateTerm)

	if lastTerm != candidateTerm {
		return lastTerm > candidateTerm
	}
	return lastIndex > candidateIndex
}

func (args *RequestVoteArgs) String() string {
	return fmt.Sprintf("Candidate-%d T%d, Last:[%d]T%d", args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
}
func (reply *RequestVoteReply) String() string {
	return fmt.Sprintf("T%d, VoteGranted: %v", reply.Term, reply.VoteGranted)
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

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (PartA, PartB).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, VoteAsked, Args=%v", args.CandidateId, args.String())

	// align the term
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if rf.currentTerm > args.Term {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Reject vote, higher term, T%d>T%d", args.CandidateId, rf.currentTerm, args.Term)
		return
	}
	if rf.currentTerm < args.Term {
		rf.becomeFollowerLocked(args.Term)
	}

	// check the votedFor
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Reject, Already voted S%d", args.CandidateId, rf.votedFor)
		return
	}

	// check log, only grant vote when the candidates have more up-to-date log
	if rf.isMoreUpToDateLocked(args.LastLogIndex, args.LastLogTerm) {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Reject Vote, S%d's log less up-to-date", args.CandidateId)
		return
	}

	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.persistLocked()
	rf.resetElectionTimerLocked()
	LOG(rf.me, rf.currentTerm, DVote, "-> S%d", args.CandidateId)
}

func (rf *Raft) electionTicker() {
	for !rf.killed() {
		// Your code here (PartA)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.role != Leader && rf.isElectionTimeoutLocked() { // 不是 Leader 且选举时间到
			// 先变成候选人
			rf.becomeCandidateLocked()
			go rf.startElection(rf.currentTerm) // 开始向别的 peers 要票
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond) // 随机睡眠
	}
}

func (rf *Raft) startElection(term int) bool {
	// 针对每个 Peer 的 RequestVote 的请求和响应处理
	votes := 0
	askVoteFromPeer := func(peer int, args *RequestVoteArgs) {
		// send RPC  to `peer` and handle the response
		reply := &RequestVoteReply{}
		ok := rf.sendRequestVote(peer, args, reply)

		// handle the response
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// 要票失败
		if !ok {
			LOG(rf.me, rf.currentTerm, DDebug, "Ask vote from %d, Lost or error", peer)
			return
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, AskVote Reply=%v", peer, reply.String())

		// align the term
		// 1. 如果对方 Term 比自己小：无视请求，通过返回值“亮出”自己的 Term
		// 2. 如果对方 Term 比自己大：乖乖跟上对方 Term，变成最“菜”的 Follower
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		// check the context
		if rf.contextLostLocked(Candidate, term) {
			LOG(rf.me, rf.currentTerm, DVote, "Lost context, abort RequestVoteReply in T%d", rf.currentTerm)
			return
		}

		// count votes
		if reply.VoteGranted {
			votes++
		}
		if votes > len(rf.peers)/2 {
			rf.becomeLeaderLocked()
			go rf.replicationTicker(term)
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// every time locked
	if rf.contextLostLocked(Candidate, term) {
		LOG(rf.me, rf.currentTerm, DVote, "Lost context, %s %d abort askVoteFromPeer in T%d", rf.role, rf.me, rf.currentTerm)
		return false
	}

	l := len(rf.log)
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			votes++
			continue
		}
		args := &RequestVoteArgs{
			Term:         term,
			CandidateId:  rf.me,
			LastLogIndex: l - 1,
			LastLogTerm:  rf.log[l-1].Term,
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, AskVote, Args=%v", peer, args.String())
		go askVoteFromPeer(peer, args)
	}

	return true
}
