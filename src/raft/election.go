package raft

import (
	"context"
	"math/rand"
	"time"
)

//candiadate action: start the election ,until one of the three follow things happends
//1.a new leader comes up
//2.get most of votes
//3.out time
func CandidateVote(rf *Raft){
	//increment currentTerm,reset election Timer,vote for self
	rf.mu.Lock()
	rf.votedFor = rf.me
	rf.lastHeartbeat = time.Now()
	rf.currentTerm++
	rf.electionClock=rand.Int63()%200+300
	rf.mu.Unlock()
	rf.persist()

	//vote variables
	ch := make(chan RequestVoteReply, len(rf.peers))		//ch for get all replys
	var args *RequestVoteArgs								//vote args
	ctx,cancelFunc:=context.WithCancel(context.Background())//close context

	rf.mu.Lock()
	args = &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.logs) - 1,
		LastLogTerm: func() int {
			if len(rf.logs)-1 < 0 {
				return -1
			} else {
				return rf.logs[len(rf.logs)-1].Term
			}
		}(),
	}
	rf.mu.Unlock()

	//send vote rpc to all other servers
	for index, _ := range rf.peers {
		if index != rf.me {
			go sendVote(rf,index,ctx,args,ch)
		}
	}

	//start receive goroutine
	go receiveVote(rf,ctx,ch)

	//wait for timeout
	time.Sleep(time.Duration(rf.electionClock)*time.Millisecond)
	cancelFunc()
	return
}

//Send vote request of Candidates
//index:index of peers
//ctx: used for close the goroutine
//ch: send reply to receiver
func sendVote(rf *Raft,index int,ctx context.Context,args *RequestVoteArgs,ch chan RequestVoteReply){
	reply := &RequestVoteReply{}
	ok := false
	//keep Calling until get the reply or something happend
	ok = rf.peers[index].Call("Raft.RequestVote", args, reply)

	//set reply,if rpc failed,term=-1&voteGranted=false
	if !ok {
		//rf.mu.Lock()
		//if rf.currentTerm < reply.Term {
		//	//此处不需要停止投票，因为不会成功
		//	rf.currentState = Follower
		//	rf.currentTerm = reply.Term
		//	rf.votedFor = -1
		//	//rf.currentLeader=-1
		//	rf.persist()
		//}
		//rf.mu.Unlock()
		reply.Term=-1
		reply.VoteGranted=false
	}
	select {
	case <-ctx.Done():
		// channel has closed
		return
	case ch<-*reply:
		//if the channel not closed,send reply
		return
	}
}


// receive vote action of candidates
// ctx: used for close goroutine
// ch: used for receive reply from func sendVote
func receiveVote(rf *Raft,ctx context.Context,ch chan RequestVoteReply){
	voteNum := 1			//number of replys
	grantNum := 1			//number of votes

	for {
		//check the vote result,until voteNum is enough or all have voted
		select {
		case <-ctx.Done():
			return
		case reply := <-ch: //have a reply
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.currentState==Follower{
				//if becomes follower,stop
				return
			}else if rf.currentTerm<reply.Term{
				//Term T > currentTerm, convert to follower, stop the receiver
				rf.currentTerm=reply.Term
				rf.currentState=Follower
				rf.lastHeartbeat=time.Now()
				return
			}
			voteNum++
			if reply.VoteGranted {
				grantNum++
			}
			if grantNum>= (len(rf.peers)+1)/2{
			//candidate get most of the votes
				rf.currentState = Leader
				rf.currentLeader = rf.me
				rf.votedFor = -1
				for i := 0; i < len(rf.nextIndex); i++ {
					rf.nextIndex[i] = len(rf.logs)
					rf.matchIndex[i] = -1
				}
				rf.persist()
				go LeaderAction(rf)
				return
			}
			//DPrintf("%d get a reply,grantNum:%d",rf.me,grantNum)
		case <-ctx.Done()://outtime when election
			DPrintf("%d outtime when election\n", rf.me)
			return
		}
	}
	rf.mu.Lock()
	DPrintf("%d becomes %v,grantnum:%d", rf.me, rf.currentState, grantNum)
	rf.mu.Unlock()
}
