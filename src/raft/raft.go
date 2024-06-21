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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"course/labgob"
	"course/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part PartD you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For PartD:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (PartA, PartB, PartC).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role        Role
	currentTerm int // 服务器已知最新的任期
	votedFor    int // 当前任期内收到选票的 candidateId，如果没有投给任何候选人，则为空

	// fields for apply loop,
	commitIndex int
	lastApplied int
	applyCond   *sync.Cond
	applyCh     chan ApplyMsg

	// used for election loop
	electionStart   time.Time
	electionTimeout time.Duration

	// log in Peer's local
	log *RaftLog

	// only used when it is Leader,
	// log view for each peer
	nextIndex  []int // 日志同步时的匹配点试探
	matchIndex []int // 日志同步成功后的匹配点记录

}

const (
	electionTimeoutMin time.Duration = 250 * time.Millisecond
	electionTimeoutMax time.Duration = 400 * time.Millisecond
	replicateInterval  time.Duration = 70 * time.Millisecond
)

type Role string

const (
	Follower  Role = "Follower"
	Candidate Role = "Candidate"
	Leader    Role = "Leader"
) // 定义全局常量

const (
	InvalidIndex int = 0
	InvalidTerm  int = 0
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (PartA).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.role == Leader
	return term, isleader // 获取 Raft 相关状态
}

// become a follower in `term`, term could not be decreased
func (rf *Raft) becomeFollowerLocked(term int) {
	// 收到的 term 小于 currentTerm 不用理会收到的包
	if term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DError, "Can't become Follower, lower term")
		return
	}

	rf.role = Follower
	shouldPersist := term != rf.currentTerm // 状态变更需持久化
	LOG(rf.me, rf.currentTerm, DLog, "%s -> Follower, For T%d->T%d", rf.role, rf.currentTerm, term)

	// important! Could only reset the `votedFor` when term increased
	// 迈入新 term 需要重置投票
	if term > rf.currentTerm {
		rf.votedFor = -1
	}
	rf.currentTerm = term

	if shouldPersist {
		rf.persistLocked()
	}
}

func (rf *Raft) becomeCandidateLocked() {
	// 已经是 Leader 不用理会收到的包
	if rf.role == Leader {
		LOG(rf.me, rf.currentTerm, DError, "Leader can't become Candidate")
		return
	}

	LOG(rf.me, rf.currentTerm, DVote, "%s -> Candidate, For T%d->T%d", rf.role, rf.currentTerm, rf.currentTerm+1)
	rf.role = Candidate
	rf.currentTerm++    // 变成 Candidate 任期 + 1
	rf.votedFor = rf.me // 变成 Candidate 先投自己

	rf.persistLocked()
}

func (rf *Raft) becomeLeaderLocked() {
	// 只有 Candidate 才能变成 Leader
	if rf.role != Candidate {
		LOG(rf.me, rf.currentTerm, DLeader,
			"%s, Only candidate can become Leader", rf.role)
		return
	}

	LOG(rf.me, rf.currentTerm, DLeader, "%s -> Leader, For T%d", rf.role, rf.currentTerm)
	rf.role = Leader

	// 初始化 matchIndex 和 nextIndex
	for peer := 0; peer < len(rf.peers); peer++ {
		rf.nextIndex[peer] = rf.log.size()
		rf.matchIndex[peer] = 0
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (PartD).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me, rf.currentTerm, DSnap, "Snap on %d", index)

	if index <= rf.log.snapLastIdx || index > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DSnap, "Could not snapshot beyond [%d, %d]", rf.log.snapLastIdx+1, rf.commitIndex)
		return
	}

	rf.log.doSnapshot(index, snapshot)
	rf.persistLocked()
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (PartB).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return 0, 0, false
	}
	rf.log.append(LogEntry{
		CommandValid: true,
		Command:      command,
		Term:         rf.currentTerm,
	})
	rf.persistLocked()

	LOG(rf.me, rf.currentTerm, DLeader, "Leader accept log [%d]T%d", rf.log.size()-1, rf.currentTerm)
	return rf.log.size() - 1, rf.currentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (PartA)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// 保证即在一个任期内，角色没有变化
func (rf *Raft) contextLostLocked(role Role, term int) bool {
	//LOG(rf.me, rf.currentTerm, DVote, "Check context, rf.currentTerm == term -> %t,  rf.role == role -> %t",
	//	rf.currentTerm == term, rf.role == role)
	//LOG(rf.me, rf.currentTerm, DVote, "Check context, !(rf.currentTerm == term && rf.role == role) -> %t",
	//	!(rf.currentTerm == term && rf.role == role))
	return !(rf.currentTerm == term && rf.role == role)
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (PartA, PartB, PartC).
	rf.role = Follower
	rf.currentTerm = 1
	rf.votedFor = -1

	// a dummy entry to avoid lots of corner checks
	rf.log = NewLog(InvalidIndex, InvalidTerm, nil, nil)

	// initialize the leader's view slice
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))

	// initialize the fields used for apply
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	// go rf.ticker()
	go rf.electionTicker()
	go rf.applyTicker()

	return rf
}
