package raft

import (
	"context"
	"math/rand"
	"time"

	//"wanggj.me/myraft/labrpc"
	//"wanggj.me/myraft/labgob"
)

//candiadate action: start the election ,until one of the three follow things happends
//1.a new leader comes up
//2.get most of votes
//3.out time
func CandidateVote(rf *Raft){

	//increment currentTerm,reset election Timer,vote for self
	rf.votedFor = rf.me
	rf.lastHeartbeat = time.Now()
	rf.currentTerm++
	rf.electionClock=rand.Int63()%200+300
	rf.persist()
	rf.mu.Unlock()


	//vote variables
	var args *RequestVoteArgs								//vote args
	ctx,cancelFunc:=context.WithCancel(context.Background())//close context
	ch:=make(chan RequestVoteReply,len(rf.peers))
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
	DPrintf("%d stop the election.",rf.me)
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
		reply.Term=-1
		reply.VoteGranted=false
	}
	DPrintf("Candidate:%d Term:%d,get vote from %d : %v",rf.me,args.Term,index,*reply)

	select {
	case <-ctx.Done():
		// channel has closed
		DPrintf("Close the goroutine %d:%d.",rf.me,index)
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
	rf.mu.Lock()
	term:=rf.currentTerm
	rf.mu.Unlock()

	for {
		//check the vote result,until voteNum is enough or all have voted
		select {
		case <-ctx.Done():
			DPrintf("%d election timeout,Term:%d",rf.me,term)
			return
		case reply := <-ch: //have a reply
			rf.mu.Lock()
			if rf.currentState!=Candidate{
				//if becomes follower,stop
				DPrintf("%d have changed into %d",rf.me,rf.currentState)
				rf.mu.Unlock()
				return
			}else if rf.currentTerm<reply.Term{
				//Term T > currentTerm, convert to follower, stop the receiver
				DPrintf("%d stop beacause bigger term.",rf.me)
				rf.currentTerm=reply.Term
				rf.currentState=Follower
				rf.lastHeartbeat=time.Now()
				rf.mu.Unlock()
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
				rf.lastHeartbeat=time.Now()
				rf.votedFor = -1
				for i := 0; i < len(rf.nextIndex); i++ {
					rf.nextIndex[i] = len(rf.logs)
					rf.matchIndex[i] = -1
				}
				rf.persist()
				rf.mu.Unlock()
				//start the leader action
				go LeaderAction(rf)
				DPrintf("%d becomes %v,grantnum:%d", rf.me, rf.currentState, grantNum)
				return
			}
			rf.mu.Unlock()
		}
	}
}
