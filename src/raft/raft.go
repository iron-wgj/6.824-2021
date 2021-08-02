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
	"bytes"
	"log"
	"math/rand"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"wanggj.me/myraft/labgob"
	"wanggj.me/myraft/labrpc"
)

type raftState int

const (
	Follower  raftState = 1
	Candidate raftState = 2
	Leader    raftState = 3
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//2A:
	currentTerm   int //current term
	votedFor      int //candidate id that received vote in current term
	currentState  raftState
	lastHeartbeat time.Time
	currentLeader int //current leader id
	electionClock int64

	//2B:
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	logs        []LogEntry
	ApplyCond   *sync.Cond
}

//a go object describe a log entry
type LogEntry struct {
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader = false
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.currentState == Leader {
		isLeader = true
	}
	rf.mu.Unlock()
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&logs) != nil {
		log.Fatal("can't decode persisted data into wanted format\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = voteFor
		rf.logs = logs
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int

	//2B
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	DPrintf("%d get a vote request from %d with term %d,my term:%d,my votefor:%d\n", rf.me, args.CandidateId, args.Term, rf.currentTerm, rf.votedFor)
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if rf.currentTerm < args.Term {
		if uptodate(rf,args.LastLogIndex, args.LastLogTerm) {
			//term is bigger and up-to-date
			rf.currentState = Follower
			rf.votedFor = args.CandidateId
			rf.lastHeartbeat = time.Now()
			rf.currentTerm = args.Term
			rf.persist()
			reply.Term = args.Term
			reply.VoteGranted = true
		} else {
			//term is bigger but not up-to-date
			rf.currentState = Follower
			rf.votedFor = -1
			//rf.lastHeartbeat = time.Now()
			rf.currentTerm = args.Term
			rf.persist()
			reply.Term = args.Term
			reply.VoteGranted = false
		}
	} else {
		if uptodate(rf,args.LastLogIndex, args.LastLogTerm) {
			//term equal but up-to-date
			if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
				//grant the vote
				reply.Term = rf.currentTerm
				reply.VoteGranted = true
				rf.votedFor = args.CandidateId
				rf.lastHeartbeat = time.Now()
			} else {
				reply.Term = rf.currentTerm
				reply.VoteGranted = false
			}
		} else {
			//term equal but not up-to-date
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		}
	}
	rf.mu.Unlock()
	return
}

func uptodate(rf *Raft,lastIndex, lastTerm int) bool {
	preIndex := len(rf.logs) - 1
	preTerm := -1
	if preIndex >= 0 {
		preTerm = rf.logs[preIndex].Term
	}
	DPrintf("%d preIndex=%d,preTerm=%d,lastIndex=%d,lastTerm=%d",rf.me,preIndex,preTerm,lastIndex,lastTerm)
	if lastTerm < preTerm {
		return false
	} else if lastTerm == preTerm {
		if lastIndex < preIndex {
			return false
		} else {
			return true
		}
	} else {
		return true
	}
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	if rf.currentState == Leader {
		rf.logs = append(rf.logs, LogEntry{
			Term:    rf.currentTerm,
			Command: command,
		})
		rf.persist()
		index = len(rf.logs)
		DPrintf("%d get a log entry,index:%d\n", rf.me, index)
		term = rf.currentTerm
		isLeader = true
	}
	rf.mu.Unlock()
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		//注意：决定发起投票和更改状态是原子操作，否则在发起投票后被改为follower然后发起投票会导致错误。
		rf.mu.Lock()
		duration := time.Now().Sub(rf.lastHeartbeat)

		//follower action: check if election clock time out
		//term increase,state become to candidate and vote for self
		//candidate action: start the vote request and check, stop when timeout
		//leader action:sleep electionTime
		if rf.currentState == Follower{
			if duration.Milliseconds() >= rf.electionClock {
				rf.currentState = Candidate
				DPrintf("%d start the election from follower,currentTerm:%d",rf.me,rf.currentTerm)
				CandidateVote(rf)
			}else{
				rf.mu.Unlock()
				time.Sleep(time.Millisecond * time.Duration(100))
			}
		}else if rf.currentState==Candidate {
			//DPrintf("%d start the vote,duration:%d,sleeptime:%d\n",rf.me,int64(duration.Milliseconds()),sleepTime)
			//start the goroutine which start the vote
			DPrintf("%d start the election from candidate,currentTerm:%d",rf.me,rf.currentTerm)
			CandidateVote(rf)
		}else{
			rf.mu.Unlock()
			time.Sleep(time.Duration(rf.electionClock)*time.Millisecond)
		}
	}
}



//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.lastHeartbeat = time.Now()
	rf.currentTerm = 0
	rf.votedFor = -1 //-1 as nil
	rf.currentState = Follower
	rf.currentLeader = -1
	rf.electionClock=rand.Int63()%200+300

	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.logs = []LogEntry{}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.ApplyCond = sync.NewCond(&rf.mu)
	// start ticker goroutine to start elections
	DPrintf("%d start the ticker\n", me)
	go rf.ticker()
	go func() {
		rf.mu.Lock()
		for {
			for rf.lastApplied < rf.commitIndex {
				applyCh <- ApplyMsg{
					Command:      rf.logs[rf.lastApplied+1].Command,
					CommandIndex: rf.lastApplied + 2,
					CommandValid: true,
				}
				rf.lastApplied++
				//DPrintf("%d applied new logs,lastApplied:%d\n",rf.me,rf.lastApplied)
			}
			rf.ApplyCond.Wait()
		}

	}()

	return rf
}
