package raft

import (
	"bytes"
	"log"
	"time"

	"wanggj.me/myraft/labgob"
	"wanggj.me/myraft/labrpc"
)

type  InstallSnapshotReply struct{
	success bool
	term int
}

type InstallSnapshotArgs struct{
	term int
	leaderId int
	lastIncludeIndex int
	lastIncludeTerm int
	data []byte
}
func (rf *Raft)InstallSnapshot(args * InstallSnapshotArgs,reply * InstallSnapshotReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.term<rf.currentTerm{
		//a leader with litte term
		reply.term=rf.currentTerm
		reply.success=false
		return
	}else {
		if rf.currentTerm<args.term{
			rf.currentTerm=args.term
			rf.currentLeader=args.leaderId
			rf.currentState=Follower
			rf.lastHeartbeat=time.Now()
			rf.persist()
		}
		rf.ApplyCh<-ApplyMsg{
			SnapshotValid: true,
			SnapshotIndex: args.lastIncludeIndex,
			SnapshotTerm: args.lastIncludeTerm,
			Snapshot: args.data,
		}
		reply.success=true
		reply.term=rf.currentTerm
		return
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.commitIndex>lastIncludedIndex{
		//snapshot is old
		return false
	} else{
		//switch to snapshot
		//set the commitIndex,lastIncludeIndex,lastIncludeIndex
		//trim the logs
		rf.trimLogs(lastIncludeIndex)
		rf.commitIndex=lastIncludeIndex
		//save the state and snapshot
		stateW := new(bytes.Buffer)
		state := labgob.NewEncoder(stateW)
		state.Encode(rf.currentTerm)
		state.Encode(rf.votedFor)
		state.Encode(rf.logs)
		state.Encode(rf.lastIncludedIndex)
		state.Encode(rf.lastIncludedTerm)
		stateData := w.Bytes()
		rf.persister.SaveStateAndSnapshot(stateData,snapshot)
		return true
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index<=rf.commitIndex{
		//trim logs and save state and snapshot
		rf.trimLogs(index)
		stateW := new(bytes.Buffer)
		state := labgob.NewEncoder(stateW)
		state.Encode(rf.currentTerm)
		state.Encode(rf.votedFor)
		state.Encode(rf.logs)
		state.Encode(rf.lastIncludedIndex)
		state.Encode(rf.lastIncludedTerm)
		stateData := w.Bytes()
		rf.persister.SaveStateAndSnapshot(stateData,snapshot)
	}else{
		log.Fatal("Service send a snapshot which has indexs larger than commitIndex, " +
			"the index=",index,",commitIndex=",rf.commitIndex)
	}
}

//trim the log:storage the tail of logs begin from index,
//then set lastIncludeIndex and lastIncludeTer
func (rf *Raft) trimLogs(index int){
	nextIndex:=index-rf.lastIncludedIndex-1
	tmpLogs:=make([]LogEntry,len(rf.logs)-nextIndex)
	copy(tmpLogs,rf.logs[nextIndex:])
	rf.logs=tmpLogs
	rf.lastIncludedIndex=index
	rf.lastIncludedTerm=rf.logs[nextIndex].Term
}