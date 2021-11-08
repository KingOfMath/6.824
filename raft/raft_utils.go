package raft

import (
	"6.824/labgob"
	"bytes"
	"math/rand"
	"sync/atomic"
	"time"
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.IsLeader()
	return term, isleader
}

// 获取最近的log，用来判断是否可以被投票
func (rf *Raft) getLastLogIdx() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	index := rf.getLastLogIdx()
	if index < 0 {
		return -1
	}
	return rf.log[index].Term
}

// 获取下一次要同步的logIndex
func (rf *Raft) getPrevLogIdx(idx int) int {
	return rf.nextIndex[idx] - 1
}

func (rf *Raft) getPrevLogTerm(idx int) int {
	prevLogIndex := rf.getPrevLogIdx(idx)
	if prevLogIndex < 0 || prevLogIndex >= len(rf.log) {
		return -1
	}
	return rf.log[prevLogIndex].Term
}

// 获取snapshot
func (rf *Raft) getLastIncludedIdx() int {
	return rf.log[0].Index
}

func (rf *Raft) getLastIncludedTerm() int {
	return rf.log[0].Term
}

// 获取除去snapshot的真实idx
func (rf *Raft) getRealLogIdx(idx int) int {
	return idx - rf.getLastIncludedIdx()
}

func (rf *Raft) IsLeader() bool {
	return rf.state == Leader
}

func (rf *Raft) IsCandidate() bool {
	return rf.state == Candidate
}

func (rf *Raft) IsFollower() bool {
	return rf.state == Follower
}

// Candidate 自己投自己
func (rf *Raft) BecomeCandidate() {
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
}

// Follower
func (rf *Raft) BecomeFollower(newTerm int) {
	rf.currentTerm = newTerm
	rf.state = Follower
	rf.votedFor = -1
}

// Leader
func (rf *Raft) BecomeLeader() {
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			// [第一次更新nextIndex]
			// 每个节点下一次应该接收的日志的index（初始化为日志长度）
			rf.nextIndex[i] = rf.getLastLogIdx() + 1
			rf.matchIndex[i] = 0
		}
	}
}

// format name
func (rf *Raft) formatName(state int) string {
	switch state {
	case Follower:
		return "Follower"
	case Leader:
		return "Leader"
	case Candidate:
		return "Candidate"
	default:
		panic("[Error] Wrong state!")
	}
}

// Kill 进程相关
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 重置时间
func (rf *Raft) reset() {
	rf.electionTimeout.Stop()
	rf.electionTimeout.Reset(time.Duration(rand.Intn(150)+300) * time.Millisecond)
}

//
// save Raft's persistent state to stable storage, where it can later be retrieved after a crash and restart.
//
func (rf *Raft) persist() {
	data := rf.getPersistState()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) getPersistState() (data []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data = w.Bytes()
	return
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	// TODO: 需要引用地址
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}
