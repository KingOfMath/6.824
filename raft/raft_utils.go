package raft

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
