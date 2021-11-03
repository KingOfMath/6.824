package raft

import (
	"math"
	"sync/atomic"
	"time"
)

// broadcast appendEntries
func (rf *Raft) broadcastHeartBeat() {

	for {
		rf.mu.Lock()
		if rf.IsLeader() == false {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()

		for i := range rf.peers {
			// 跳过自己
			if rf.me != i {
				go func(idx int) {

					rf.mu.Lock()
					args := &AppendEntriesArgs{
						Term:     rf.currentTerm,
						LeaderId: rf.me,
						// 传入NextIndex之前的Log状态
						PrevLogIndex: rf.getPrevLogIdx(idx),
						PrevLogTerm:  rf.getPrevLogTerm(idx),
						// 1. 拼接slice要把数组用...打散
						// 2. 传入Leader的需要复制的log
						Entries:      append(make([]Log, 0), rf.log[rf.nextIndex[idx]:]...),
						LeaderCommit: rf.commitIndex,
					}
					rf.mu.Unlock()

					reply := &AppendEntriesReply{}
					ok := rf.sendAppendEntries(idx, args, reply)

					rf.mu.Lock()
					defer rf.mu.Unlock()

					if !ok || !rf.IsLeader() || rf.currentTerm != args.Term {
						return
					}

					if reply.Term > rf.currentTerm {
						rf.BecomeFollower(reply.Term)
						return
					}

					if reply.Success {
						// update nextIndex and matchIndex for follower
						// follower的index应该被强制和leader同步
						rf.matchIndex[idx] = rf.getLastLogIdx()
						rf.nextIndex[idx] = rf.matchIndex[idx] + 1
						// update commitIndex
						rf.commitN()
						return
					} else {
						// decrement nextIndex and retry
						rf.nextIndex[idx] = reply.NextIndex
					}
				}(i)
			}
		}
		time.Sleep(time.Duration(HeartBeatTimeout))
	}
}

// AppendEntries 心跳逻辑，用于RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false

	// Cond1: 如果Leader很菜，自己强，直接拒绝Leader请求
	if args.Term < rf.currentTerm {
		return
	}

	// Cond2: 如果自己比当前的Leader菜，可能自己还是Candidate或者Leader，当个follower
	rf.BecomeFollower(args.Term)
	// CASE2: 重置时间片
	defer rf.reset()

	// Cond3: 如果preLogIndex长度大于当前日志长度，说明follower缺失日志
	if args.PrevLogIndex > rf.getLastLogIdx() {
		reply.Success = false
		// 重试，下一次的PrevLogIndex就直接从LastLogIdx开始走
		reply.NextIndex = rf.getLastLogIdx() + 1
		return
	}

	// Cond4: 如果preLogIndex的Term和当前不相同，说明日志冲突，需要找到冲突点
	// TODO: 不是同一个idx下的Term嘛？为什么会有冲突？
	prevLogTerm := rf.log[args.PrevLogIndex].Term
	if args.PrevLogTerm != prevLogTerm {
		reply.Success = false
		// 找到冲突的index，告诉leader之后要从哪里发送日志
		// 找到第一个当前prevLogTerm起始位置，作为当前冲突term的起点
		for i := args.PrevLogIndex - 1; i >= 0; i-- {
			if rf.log[i].Term != prevLogTerm {
				reply.NextIndex = i + 1
				break
			}
		}
		return
	}

	// Cond5: 正常更新日志： 截取直到当前Leader传入的PrevLogIndex前的已有日志，新增Leader日志
	rf.log = rf.log[:args.PrevLogIndex+1]
	rf.log = append(rf.log, args.Entries...)
	reply.NextIndex = rf.getLastLogIdx() + 1

	// Cond6: 更新commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(rf.getLastLogIdx())))
		rf.applyMsg()
	}
	reply.Success = true
	return
}

// 发送 entries
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) (ok bool) {
	ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return
}

// 选择超过多数matchIndex的N，更新为新的最新的commitIndex
// 更新的是Leader的commitIndex
func (rf *Raft) commitN() {
	// N要比commitIndex大
	for N := rf.commitIndex + 1; N < len(rf.log); N++ {
		var count int32 = 0
		for _, idx := range rf.matchIndex {
			if idx >= N {
				count++
			}
		}
		if atomic.LoadInt32(&count) > rf.winVoteThreshold && rf.log[N].Term == rf.currentTerm {
			rf.commitIndex = N
			rf.applyMsg()
			break
		}
	}
}

// 将所有还没有commit的log传入state machine
// 用在commitIndex更新的时候，保持状态机的一致
func (rf *Raft) applyMsg() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- msg
	}
}
