package raft

import (
	"sync/atomic"
)

// 选举循环,用两个time.Timer接收
func (rf *Raft) startEventLoop() {
	for !rf.killed() {
		select {
		case <-rf.electionTimeout.C:
			rf.mu.Lock()
			// Leader在挂机前只能用来发心跳
			if rf.IsLeader() {
				rf.mu.Unlock()
				continue
			}
			rf.mu.Unlock()

			// 重新开始选举：避免由于延迟或丢失的RPC导致系统停顿
			rf.startElection()
		}
	}
}

// 选举具体逻辑
func (rf *Raft) startElection() {
	// 以参选人开始选举
	rf.mu.Lock()
	rf.BecomeCandidate()
	rf.persist()
	rf.mu.Unlock()

	// CASE1: 重置时间片
	rf.reset()

	// 发送投票
	go rf.broadcastRequestVote()
}

// broadcast votes
func (rf *Raft) broadcastRequestVote() {
	var voteCount int32 = 1
	rf.mu.Lock()
	// args
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIdx(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	rf.mu.Unlock()
	for i := range rf.peers {
		if rf.me != i {
			go func(idx int) {
				reply := &RequestVoteReply{}
				// 发送RPC的时候不能加锁！
				ok := rf.sendRequestVote(idx, args, reply)

				if ok {
					// lock
					rf.mu.Lock()
					defer rf.mu.Unlock()
					defer rf.persist()

					if rf.IsCandidate() == false {
						return
					}

					// 假如对方既没有投自己，自己还比别人垃圾
					if reply.Term > rf.currentTerm {
						rf.BecomeFollower(reply.Term)
						return
					}

					// 得到选票
					if reply.VoteGranted {
						atomic.AddInt32(&voteCount, 1)

						// 竞选胜利
						if atomic.LoadInt32(&voteCount) > rf.winVoteThreshold {
							rf.BecomeLeader()

							// 立即发送心跳，让大家变follower
							go rf.broadcastHeartBeat()
						}
					}
				}
			}(i)
		}
	}
}

// vote
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// Logic1: 假如自己比选举人垃圾，退选
	if args.Term > rf.currentTerm {
		rf.BecomeFollower(args.Term)
	}

	// Logic2: 假如选举人比自己还垃圾，直接让他滚
	if args.Term < rf.currentTerm {
		return
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// 日志更新才投票
		if args.LastLogTerm > rf.getLastLogTerm() ||
			(args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIdx()) {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.state = Follower
			// CASE3: 重置时间片
			rf.reset()
		}
	}
	return
}

// 发送Vote
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) (ok bool) {
	ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	return
}
