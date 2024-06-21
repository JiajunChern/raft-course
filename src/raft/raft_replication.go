package raft

import (
	"fmt"
	"sort"
	"time"
)

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConfilictIndex int
	ConfilictTerm  int
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	// used to probe the match point
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry

	// used to update the follower's commitIndex
	LeaderCommit int
}

func (args *AppendEntriesArgs) String() string {
	return fmt.Sprintf("Leader-%d, T%d, Prev:[%d]T%d, (%d, %d], CommitIdx: %d",
		args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm,
		args.PrevLogIndex, args.PrevLogIndex+len(args.Entries), args.LeaderCommit)
}
func (reply *AppendEntriesReply) String() string {
	return fmt.Sprintf("T%d, Sucess: %v, ConflictTerm: [%d]T%d", reply.Term, reply.Success, reply.ConfilictIndex, reply.ConfilictTerm)
}

// 心跳接收方在收到心跳时，只要 Leader 的 term 不小于自己，就对其进行认可，变为 Follower，并重置选举时钟，承诺一段时间内不发起选举。
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, Appended, Args=%v", args.LeaderId, args.String())
	reply.Term = rf.currentTerm
	reply.Success = false
	// align the term
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log", args.LeaderId)
		return
	}
	if args.Term >= rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	// reset the timer
	defer func() {
		rf.resetElectionTimerLocked()
		if !reply.Success {
			LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Follower Conflict: [%d]T%d", args.LeaderId, reply.ConfilictIndex, reply.ConfilictTerm)
			LOG(rf.me, rf.currentTerm, DDebug, "Follower log=%v", rf.log.String())
		}
	}()

	// return failure if the previous log not matched
	if args.PrevLogIndex >= rf.log.size() {
		reply.ConfilictIndex = rf.log.size()
		reply.ConfilictTerm = InvalidTerm
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject Log, Follower log too short, Len:%d <= Prev:%d", args.LeaderId, rf.log.size(), args.PrevLogIndex)
		return
	}
	if rf.log.at(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.ConfilictTerm = rf.log.at(args.PrevLogIndex).Term
		reply.ConfilictIndex = rf.log.firstFor(reply.ConfilictTerm)
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject Log, Prev log not match, [%d]: T%d != T%d", args.LeaderId, args.PrevLogIndex, rf.log.at(args.PrevLogIndex).Term, args.PrevLogTerm)
		return
	}

	// append the leader logs to local
	rf.log.appendFrom(args.PrevLogIndex, args.Entries)
	LOG(rf.me, rf.currentTerm, DLog2, "Follower append logs: (%d, %d]", args.PrevLogIndex, args.PrevLogIndex+len(args.Entries))
	rf.persistLocked()
	reply.Success = true

	// TODO: handle the args.LeaderCommit
	// update the commit index if needed and indicate the apply loop to apply
	if args.LeaderCommit > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DApply, "Follower update the commit index %d->%d", rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = args.LeaderCommit
		if rf.commitIndex >= rf.log.size() {
			rf.commitIndex = rf.log.size() - 1
		}
		rf.applyCond.Signal()
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) getMajorityIndexLocked() int {
	// TODO(spw): may could be avoided copying
	tmpIndexes := make([]int, len(rf.matchIndex))
	copy(tmpIndexes, rf.matchIndex)
	sort.Ints(tmpIndexes)
	majorityIdx := (len(tmpIndexes) - 1) / 2
	LOG(rf.me, rf.currentTerm, DDebug, "Match index after sort: %v, majority[%d]=%d", tmpIndexes, majorityIdx, tmpIndexes[majorityIdx])
	return tmpIndexes[majorityIdx] // min -> max
}

// 单轮心跳：对除自己外的所有 Peer 发送一个心跳 RPC
func (rf *Raft) startReplication(term int) bool {

	// 单次 RPC：对某个 Peer 来发送心跳，并且处理 RPC 返回值
	replicateToPeer := func(peer int, args *AppendEntriesArgs) {
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, args, reply)

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Lost or crashed", peer)
			return
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Append, Reply=%v", peer, reply.String())

		// align the term
		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		// check context lost
		if rf.contextLostLocked(Leader, term) {
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Context Lost, T%d:Leader->T%d:%s", peer, term, rf.currentTerm, rf.role)
			return
		}

		// probe the lower index if the prev log not matched
		if !reply.Success {
			prevIndex := rf.nextIndex[peer]

			idx := rf.nextIndex[peer] - 1
			term := rf.log.at(idx).Term
			for idx > 0 && rf.log.at(idx).Term == term {
				idx--
			}
			rf.nextIndex[peer] = idx + 1

			// avoid the late reply move the nextIndex forward again
			if rf.nextIndex[peer] > prevIndex {
				rf.nextIndex[peer] = prevIndex
			}
			// LOG(rf.me, rf.currentTerm, DLog, "Log not matched in %d, Update next=%d", args.PrevLogIndex, rf.nextIndex[peer])
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Not matched at Prev=[%d]T%d, Try next Prev=[%d]T%d", peer, args.PrevLogIndex, rf.log.at(args.PrevLogIndex).Term, rf.nextIndex[peer]-1, rf.log.at(rf.nextIndex[peer]-1).Term)
			LOG(rf.me, rf.currentTerm, DDebug, "Leader log=%v", rf.log.String())
			return
		}

		// update the match/next index if log appended successfully
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1

		// TODO: need compute the new commitIndex here,
		// but we leave it to the other chapter
		// update the commmit index if log appended successfully
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1 // important: must update
		majorityMatched := rf.getMajorityIndexLocked()
		if majorityMatched > rf.commitIndex && rf.log.at(majorityMatched).Term == rf.currentTerm {
			LOG(rf.me, rf.currentTerm, DApply, "Leader update the commit index %d->%d", rf.commitIndex, majorityMatched)
			rf.commitIndex = majorityMatched
			rf.applyCond.Signal()
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLeader, "Leader[T%d] -> %s[T%d]", term, rf.role, rf.currentTerm)
		return false
	}

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			// Don't forget to update Leader's matchIndex
			rf.matchIndex[peer] = rf.log.size() - 1
			rf.nextIndex[peer] = rf.log.size()
			continue
		}

		prevIdx := rf.nextIndex[peer] - 1
		prevTerm := rf.log.at(prevIdx).Term

		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      rf.log.tail(prevIdx + 1),
			LeaderCommit: rf.commitIndex,
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Append, %v", peer, args.String())
		go replicateToPeer(peer, args)
	}

	return true
}

// 心跳 Loop：在当选 Leader 后起一个后台线程，等间隔的发送心跳/复制日志
func (rf *Raft) replicationTicker(term int) {
	for !rf.killed() {
		ok := rf.startReplication(term)
		if !ok {
			return
		}

		time.Sleep(replicateInterval)
	}
}
