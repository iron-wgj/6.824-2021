package raft

import (
	"6.824/labrpc"
	"time"
)

//struct and function fo appendEntries RPC
type AppendEntries struct {
	//2A:
	Term     int //leader's term
	LeaderId int //so followers can redirect client

	//2B:
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	Heartbeat    bool
	LeaderCommit int
}

type AppendReply struct {
	Term             int  //currentTerm,for leader to update itself
	Success          bool //2B:if match the prevLog
	ConflictTerm     int
	ConflictMinIndex int
}

//AppendEntries RPC: for heartbeat and entries append
func (rf *Raft) AppendEntries(args *AppendEntries, reply *AppendReply) {
	rf.mu.Lock()
	DPrintf("%d get a appendentries,term:%d,my term:%d,heartbeat:%v,leadercommitIndex:%d,commitindex:%d,preIndex:%d,preTerm:%d,logs:%v\n", rf.me, args.Term, rf.currentTerm, args.Heartbeat, args.LeaderCommit, rf.commitIndex, args.PrevLogIndex, args.PrevLogTerm, args.Entries)
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
	} else {
		//because an appendentry must be sent by a leader,
		//thus condition of equal terms is also seen as a leader comeout
		rf.lastHeartbeat = time.Now()
		rf.currentTerm = args.Term
		rf.currentState = Follower
		if rf.currentTerm < args.Term {
			rf.votedFor = -1
		}
		rf.currentLeader = args.LeaderId
		rf.persist()
		rf.mu.Unlock()

		//handle the entry
		if !args.Heartbeat {
			//not a heartbeat
			//DPrintf("%d get a appendentries,term:%d,my term:%d,heartbeat:%v,leadercommitIndex:%d,commitindex:%d,logs:%v\n", rf.me,args.Term,rf.currentTerm,args.Heartbeat,args.LeaderCommit,rf.commitIndex,args.Entries)
			if func(prevIndex int, pervTerm int) bool {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if prevIndex >= len(rf.logs) {
					return false
					//DPrintf("%d append entry %d fail:pre entry loss\n",rf.me,prevIndex+1)
				} else if prevIndex < 0 {
					//from the begining
					return true
				} else {
					if rf.logs[prevIndex].Term != pervTerm {
						//preTerm not match
						//DPrintf("%d append entry %d fail:term not match\n",rf.me,prevIndex+1)
						return false
					}
				}
				return true
			}(args.PrevLogIndex, args.PrevLogTerm) {
				// match the prevlogentry
				rf.mu.Lock()
				if args.PrevLogIndex < 0 {
					rf.logs = args.Entries
				} else {
					rf.logs = append(rf.logs[:args.PrevLogIndex+1], args.Entries...)
				}
				if args.LeaderCommit > rf.commitIndex {
					rf.commitIndex = func(a, b int) int {
						if a < b {
							return a
						} else {
							return b
						}
					}(len(rf.logs)-1, args.LeaderCommit)
					rf.ApplyCond.Broadcast()
				}
				rf.persist()
				reply.Success = true
				reply.Term = rf.currentTerm
				rf.mu.Unlock()
				//DPrintf("%d get a log,now log length:%d\n",rf.me,len(rf.logs))
			} else {
				rf.mu.Lock()
				if args.PrevLogIndex >= len(rf.logs) {
					reply.ConflictMinIndex = len(rf.logs)
					reply.ConflictTerm = -1
				} else {
					minIndex := args.PrevLogIndex
					term := rf.logs[minIndex].Term
					for minIndex > 0 {
						if rf.logs[minIndex-1].Term == term {
							minIndex--
						} else {
							break
						}
					}
					reply.ConflictTerm = term
					reply.ConflictMinIndex = minIndex
				}
				DPrintf("%d logs:%v\n", rf.me, rf.logs)
				reply.Term = rf.currentTerm
				rf.mu.Unlock()
				reply.Success = false
			}
		} else {
			//if is a heartbeat
			rf.mu.Lock()
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = func(a, b int) int {
					if a < b {
						return a
					} else {
						return b
					}
				}(len(rf.logs)-1, args.LeaderCommit)
				rf.ApplyCond.Broadcast()
			}
			reply.Term = rf.currentTerm
			rf.mu.Unlock()

			reply.Success = true
		}

	}
	return
}

//HeartBeat function leader used for informe the followers to restart the election timers
func (rf *Raft) HeartBeat(server int) {
	rf.mu.Lock()
	args := AppendEntries{
		Term:      rf.currentTerm,
		LeaderId:  rf.me,
		Heartbeat: true,
		LeaderCommit: func(a, b int) int {
			if a < b {
				return a
			} else {
				return b
			}
		}(rf.commitIndex, rf.matchIndex[server]), //leader don't commit log entries of last terms ,but the commited entries can be recommitted
	}
	rf.mu.Unlock()
	var reply AppendReply
	//DPrintf("send a heartbeat to %d\n",server)
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	if ok {
		if !reply.Success {
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.currentState = Follower
				rf.votedFor = -1
				rf.persist()
			}
			rf.mu.Unlock()
		}
		return
	} else {
		return
	}
}

//when a server becomes a leader, it do the leaderAction, including send new entries and heartbeats
func LeaderAction(rf *Raft) {
	//create the goroutines that send entries
	for index, peer := range rf.peers {
		if index != rf.me {
			go func(index int, peer *labrpc.ClientEnd) {
				matched := false
				rf.mu.Lock()
				term := rf.currentTerm
				rf.mu.Unlock()
				for {
					rf.mu.Lock()
					currentState := rf.currentState
					currentTerm := rf.currentTerm
					lastIndex := len(rf.logs) - 1
					nextIndex := rf.nextIndex[index]
					//entryEnd:=nextIndex+1
					//for entryEnd<=lastIndex{
					//	if rf.logs[entryEnd].Term==rf.logs[nextIndex].Term{
					//		entryEnd++
					//	}else{
					//		break
					//	}
					//}
					rf.mu.Unlock()
					//judge if it is a leader
					if currentState != Leader || currentTerm > term {
						DPrintf("%d log replication goroutine of %d return\n", rf.me, index)
						return
					}
					//if has new log entries
					if lastIndex >= nextIndex {
						//存在新的log
						rf.mu.Lock()
						entryEnd := nextIndex + 1
						if matched {
							entryEnd = lastIndex + 1
						}
						Entries := make([]LogEntry, 0)
						Entries = append(Entries, rf.logs[nextIndex:entryEnd]...)
						//DPrintf("%d nextIndex:%d,lastIndex:%d,entryEnd:%d,entries:%d\n",index,nextIndex,lastIndex,entryEnd, len(Entries))
						args := AppendEntries{
							Term:         rf.currentTerm,
							LeaderId:     rf.me,
							PrevLogIndex: nextIndex - 1, //the immediately pre entry of entries
							PrevLogTerm: func() int {
								if rf.nextIndex[index] <= 0 {
									return -1
								} else {
									return rf.logs[rf.nextIndex[index]-1].Term
								}
							}(),
							Heartbeat: false,
							Entries:   Entries, //log entries of a whole term
							LeaderCommit: func(a, b int) int {
								if a < b {
									return a
								} else {
									return b
								}
							}(rf.commitIndex, rf.matchIndex[index]),
						}
						rf.mu.Unlock()
						reply := AppendReply{
							Term:    -1,
							Success: false,
						}
						//DPrintf("%d append a entries to %d\n",rf.me,index)
						if peer.Call("Raft.AppendEntries", &args, &reply) {
							if reply.Success {
								matched = true
								rf.mu.Lock()
								rf.matchIndex[index] = entryEnd - 1
								DPrintf("matchindex %d becomes to %d,now mathcindex:%v, commitIndex:%d\n", index, rf.matchIndex[index], rf.matchIndex, rf.commitIndex)
								n := rf.matchIndex[index]
								if n > rf.commitIndex {
									count := 0
									for _, replicaIndex := range rf.matchIndex {
										if n <= replicaIndex {
											count++
										}
									}
									if count >= (len(rf.peers)-1)/2 && rf.logs[n].Term == rf.currentTerm {
										rf.commitIndex = n
										//inform the server new commit index
										for i, _ := range rf.peers {
											if i != rf.me {
												go rf.HeartBeat(i)
											}
										}
										rf.ApplyCond.Broadcast()
									}
								} else {
									//if don't get the last commit index but acheive the last term, set the heartbeat
									//if rf.logs[n].Term>=rf.currentTerm{
									//	go rf.HeartBeat(index)
									//}
									go rf.HeartBeat(index)
								}
								rf.nextIndex[index] = entryEnd
								DPrintf("%d change nextIndex to %d,mathced:%v,entryEnd:%d\n", index, rf.nextIndex[index], matched, entryEnd)
								rf.mu.Unlock()
							} else {
								rf.mu.Lock()
								if rf.currentTerm < reply.Term {
									//outdate
									rf.currentTerm = reply.Term
									rf.currentState = Follower
									rf.votedFor = -1
									rf.persist()
									//5.8 22:43 if outdate, return
									rf.mu.Unlock()
									return
									///////////
								} else {
									DPrintf("append to %d faild,reply:%v\n", index, reply)
									if reply.ConflictMinIndex < 0 {
										rf.nextIndex[index] = 0
									} else {
										if rf.logs[reply.ConflictMinIndex].Term == reply.ConflictTerm {
											//if last same term entry have the same index, it is mathced
											rf.nextIndex[index] = reply.ConflictMinIndex + 1
										} else {
											//else change to next term
											rf.nextIndex[index] = reply.ConflictMinIndex
										}
									}
									//find min index of last term
									//rf.nextIndex[index]--
									//currentIndexTerm:=rf.logs[rf.nextIndex[index]].Term
									//for rf.nextIndex[index]>0{
									//	if rf.logs[rf.nextIndex[index]-1].Term==currentIndexTerm{
									//		rf.nextIndex[index]--
									//	}else{
									//		break
									//	}
									//}
								}
								rf.mu.Unlock()
							}
						} else {
							//if RPC failed, sleep for ten ms and retry
							time.Sleep(10 * time.Millisecond)
						}
					} else {
						time.Sleep(10 * time.Millisecond)
					}
				}
				DPrintf("log ")
			}(index, peer)
		}
	}
	for {
		//if is the leader, then every 150 miliseconds send a heartbeat
		rf.mu.Lock()
		if rf.currentState == Leader {
			rf.lastHeartbeat = time.Now()
			rf.mu.Unlock()
			for index, _ := range rf.peers {
				if index != rf.me {
					go rf.HeartBeat(index)
				}
			}
		} else {
			rf.mu.Unlock()
			break
		}
		time.Sleep(150 * time.Millisecond)
	}
}
