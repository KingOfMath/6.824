package raft

import (
	"6.824/labrpc"
	"sync"
	"time"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Raft roles
const (
	Follower = iota
	Candidate
	Leader
	HeartBeatTimeout time.Duration = 100 * time.Millisecond
)

//
// 论文左上角：State
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	state            int
	electionTimeout  *time.Timer
	winVoteThreshold int32

	// Persistent state on all servers.
	currentTerm int
	votedFor    int
	log         []Log

	// Volatile state on all servers.
	commitIndex int
	lastApplied int

	// Volatile state on leaders.
	nextIndex  []int
	matchIndex []int

	snapshotLastIndex int
	snapshotLastTerm  int
	applyCh           chan ApplyMsg
}

type Log struct {
	Index   int
	Term    int
	Command interface{}
}

// 论文左下角：AppendEntries
type AppendEntriesArgs struct {
	Term         int   // leader’s term
	LeaderId     int   // so follower can redirect clients
	PrevLogTerm  int   // index of log entry immediately preceding new ones
	PrevLogIndex int   // term of prevLogIndex entry
	Entries      []Log // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int   // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

//
// 论文右上角：RequestVote
// example RequestVote RPC arguments structure.
// example RequestVote RPC reply structure.
//
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

// Part 7
// Snapshot
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}
