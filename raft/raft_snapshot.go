package raft

import "fmt"

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 放弃老的snapshot
	if lastIncludedIndex < rf.snapshotLastIndex {
		return false
	}
	rf.trimLog(lastIncludedIndex, lastIncludedTerm, snapshot)

	fmt.Println("Cond trim:", rf.log)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.log[index-rf.snapshotLastIndex].Term
	rf.trimLog(index, term, snapshot)

	fmt.Println("first trim:", rf.log)
}

// trim log
func (rf *Raft) trimLog(idx int, term int, snapshot []byte) {
	diff := idx - rf.snapshotLastIndex
	// 如果超过了日志长，新开一个snapshot; 否则用真实索引拼接
	if diff >= len(rf.log) {
		rf.log = append(make([]Log, 1), Log{
			Term:  idx,
			Index: term,
		})
	} else if diff > 0 {
		rf.log = rf.log[diff:]
	}
	rf.snapshotLastIndex = idx
	rf.snapshotLastTerm = term
	rf.persister.SaveStateAndSnapshot(rf.getPersistState(), snapshot)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	rf.BecomeFollower(args.Term)
	defer rf.reset()
	rf.persist()

	// 放弃老的snapshot
	if args.LastIncludedIndex <= rf.snapshotLastIndex {
		return
	}

	// 更新状态
	rf.applyCh <- ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
		Snapshot:      args.Data,
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) (ok bool) {
	ok = rf.peers[server].Call("Raft.InstallSnapshot", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || !rf.IsLeader() || args.Term != rf.currentTerm {
		return ok
	}

	if reply.Term > rf.currentTerm {
		rf.BecomeFollower(reply.Term)
		rf.persist()
		return
	}

	// 更新匹配数据
	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex
	return
}

// 传输的数据
func (rf *Raft) getInstallSnapshotArgs() *InstallSnapshotArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	args := &InstallSnapshotArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LastIncludedIndex = rf.snapshotLastIndex
	args.LastIncludedTerm = rf.snapshotLastTerm
	args.Data = rf.persister.ReadSnapshot()

	return args
}
