package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new Logs entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the Logs, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"
)
import "sync/atomic"
import "6.824/labrpc"
import "6.824/labgob"



type NodeState int
const (
	follower  NodeState = iota
	candidate
	leader
)


//
// as each Raft peer becomes aware that successive Logs entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed Logs entry.
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

type Log struct{
	Command interface{}
	Term uint64
}

type ServiceMessage struct{
	MessageType string
	Obj interface{}
}

type LeaderChange struct {
	CommitIndex int
	LogIndex int
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

	currentState NodeState

	CurrentTerm uint64
	VotedFor    int
	Logs        []Log

	commitIndex       uint64
	lastApplied       uint64
	lastSnapshotIndex uint64
	lastSnapshotTerm uint64

	nextIndex     map[int]uint64
	matchIndex    map[int]uint64
	idToAppending map[int]bool

	hasReceiveRpc bool // if received Rpc between election timeout
	receivedVotes map[int]bool

	electionContext  context.Context
	cancelFunc       context.CancelFunc
	hasSendHeartbeat bool
	applyCh          chan ApplyMsg
	snapshot         []byte
}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {


	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("raft %s check leader %v", rf.who(), rf.currentState == leader)
	return int(rf.CurrentTerm), rf.currentState == leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Logs)
	data := w.Bytes()

	snapshot := new(bytes.Buffer)
	ssEncode := labgob.NewEncoder(snapshot)
	ssEncode.Encode(rf.snapshot)
	ssEncode.Encode(rf.lastSnapshotIndex)

	rf.persister.SaveStateAndSnapshot(data, snapshot.Bytes())
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term uint64
	var votedFor int
	var log []Log
	if d.Decode(&term) != nil ||
	   d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil{
	} else {
	  rf.CurrentTerm = term
	  rf.VotedFor = votedFor
	  rf.Logs = log
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index > int(rf.commitIndex){
		DPrintf("raft %s got an invalid snapshot index %d, commit index is %d",
			rf.who(), index, rf.commitIndex)
		return
	}

	logIndex := index - int(rf.lastSnapshotIndex)

	rf.lastSnapshotTerm = rf.Logs[logIndex - 1].Term
	rf.lastSnapshotIndex = uint64(index)
	rf.snapshot = snapshot
	rf.Logs = rf.Logs[logIndex:]

	rf.persist()
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term uint64
	CandidateId int
	LastLogIndex uint64
	LastLogTerm uint64
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term uint64
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("raft %s got request vote %v", rf.who(), args)
	rf.hasReceiveRpc = true

	if rf.CurrentTerm > args.Term{
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
	}else{
		if rf.CurrentTerm < args.Term{
			rf.CurrentTerm = args.Term
			rf.VotedFor = -1
			rf.changeToFollower()
		}

		if rf.VotedFor == -1 &&
			((args.LastLogTerm > rf.getLastLogTerm()) ||
				(args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex())){
			reply.VoteGranted = true
			reply.Term = rf.CurrentTerm
			rf.VotedFor = args.CandidateId
			//rf.persist()
		}else{
			reply.VoteGranted = false
			reply.Term = rf.CurrentTerm
		}
	}
}

type AppendEntriesArgs struct {
	Term uint64
	LeaderId int
	PrevLogIndex uint64
	PrevLogTerm uint64
	Entries []Log
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term uint64
	Success bool
	XTerm  uint64
	XIndex uint64
	XLen   int64
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("raft %s got append entries %v", rf.who(), args)
	rf.hasReceiveRpc = true
	if rf.CurrentTerm > args.Term{
		reply.Success = false
		reply.Term = rf.CurrentTerm
	}else{
		if rf.CurrentTerm < args.Term{
			rf.CurrentTerm = args.Term
			rf.VotedFor = -1
			rf.changeToFollower()
		}
		reply.Term = rf.CurrentTerm

		// first append, follow all
		if args.PrevLogIndex == 0{
			reply.Success = true

			// need modify logs
			index := uint64(len(args.Entries))
			if rf.lastSnapshotIndex < index{
				rf.Logs = make([]Log, 0)
				rf.Logs = append(rf.Logs, args.Entries[rf.lastSnapshotIndex :]...)
				rf.persist()
			}
		}else{
			//follower index not found, get the last prev index
			if rf.getLastLogIndex() < args.PrevLogIndex{
				reply.Success = false
				reply.XLen = int64(rf.getLastLogIndex())
				//DPrintf("raft %s xlen to %d", rf.who(), reply.XLen)
			}else{
				//skip the index already in snapshot
				prevLogIndex := args.PrevLogIndex
				syncLogs := args.Entries
				if prevLogIndex <= rf.lastSnapshotIndex{
					syncOffset := rf.lastSnapshotIndex - prevLogIndex
					prevLogIndex = rf.lastSnapshotIndex

					if syncOffset > uint64(len(syncLogs)){
						syncLogs = make([]Log, 0, 0)
					}else{
						syncLogs = syncLogs[syncOffset:]
					}
				}


				prevLogTerm := rf.getLogTermAtIndex(prevLogIndex)

				//follower not match
				if prevLogTerm != args.PrevLogTerm{
					reply.Success = false
					reply.XTerm = prevLogTerm
					reply.XIndex = prevLogIndex
					index := prevLogIndex - 1
					for index > 0{
						if rf.getLogTermAtIndex(index) == prevLogTerm{
							reply.XIndex = index
						}else{
							break
						}
						index -= 1
					}
				}else{
					//follower match
					reply.Success = true

					resLen := prevLogIndex + uint64(len(syncLogs))
					for index, entry := range syncLogs{
						pivot := prevLogIndex + uint64(index) + 1

						lastLogIndex := rf.getLastLogIndex()
						if lastLogIndex < pivot{
							rf.Logs = append(rf.Logs, entry)
						}else{
							rf.setLogAtIndex(pivot, entry)
						}
					}

					// remove not valid logs
					if rf.getLastLogIndex() > resLen && resLen > 0{
						DPrintf("raft %s need remove not valid logs from index %d, all logs are %v",
							rf.who(), resLen, rf.Logs)

						if rf.getLogTermAtIndex(resLen + 1) < rf.getLogTermAtIndex(resLen){
							rf.deleteLogsAfterIndex(resLen)
						}
					}

					rf.persist()
				}
			}
		}

		//change commitIndex
		if reply.Success{
			if args.LeaderCommit > rf.commitIndex{
				rf.commitLogs(min(args.LeaderCommit, rf.getLastLogIndex()))
			}
		}


		DPrintf("raft %s got append entries %v ========after", rf.who(), args)
	}
}

func min(u uint64, u2 uint64) uint64 {
	if u < u2{
		return u
	}else{
		return u2
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
// agreement on the next command to be appended to Raft's Logs. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft Logs, since the leader
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
	isLeader := true

	// Your code here (2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Printf("enter start for %d \n", rf.me)
	index = int(rf.getLastLogIndex() + 1)
	term = int(rf.CurrentTerm)
	isLeader = rf.currentState == leader
	if isLeader{
			DPrintf("raft %s append log %v", rf.who(), command)
			rf.Logs = append(rf.Logs, Log{command, rf.CurrentTerm})
			rf.persist()
			rf.sendAllAppendEntries(false)
	}

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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.cancelFunc()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) getElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(150) + 150) * time.Millisecond
}
// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
	}
}

func (rf *Raft) followerElectionTimeout(ctx context.Context) {
	//get random expires timeout

	//DPrintf("raft %s followerElectionTimeou", rf.who())
	select{
		case <- time.After(rf.getElectionTimeout()):
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if !rf.hasReceiveRpc{
				// change to candidate
				rf.changeToCandidate()
			} else{
				//continue
				rf.hasReceiveRpc = false
				rf.cancelFunc()
				rf.electionContext, rf.cancelFunc = context.WithCancel(context.Background())
				go func(ctx context.Context){rf.followerElectionTimeout(ctx)}(rf.electionContext)
			}


		case <- ctx.Done():
			//_, _ = DPrintf("follower election time done")

	}

}

func (rf *Raft) candidateElectionTimeout(ctx context.Context){
	select{
		case <- time.After(rf.getElectionTimeout()):
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.changeToCandidate()
		case <- ctx.Done():
			//_, _ = DPrintf("candidate election time done")
	}
}

func (rf *Raft) leaderElectionTimeout(ctx context.Context) {
	select{
	case <- time.After(rf.getElectionTimeout() / 3):
		rf.mu.Lock()
		defer rf.mu.Unlock()

		DPrintf("raft %s leader election timeout", rf.who())
		rf.sendAllAppendEntries(true)

		//continue
		for k, _ := range rf.idToAppending{
			rf.idToAppending[k] = false
		}
		rf.cancelFunc()
		rf.electionContext, rf.cancelFunc = context.WithCancel(context.Background())
		go func(ctx context.Context){rf.leaderElectionTimeout(ctx)}(rf.electionContext)
	case <- ctx.Done():
		//_, _ = DPrintf("leader election time done")
	}
}

//changeTo function need lock outside by caller
func (rf *Raft) changeToFollower(){
	DPrintf("raft %s change to follower ", rf.who())
	// leader to follower, need notify service commit index, log index, make not commit service to retry
	if rf.currentState == leader{
		DPrintf("raft %s change to follower from leader, need notify", rf.who())
		rf.applyCh <- ApplyMsg{
			false,
			ServiceMessage{"leaderChange",
				LeaderChange{int(rf.commitIndex), int(rf.getLastLogIndex())}},
			int(rf.commitIndex),
			false,
			make([]byte, 0),
			-1,
			-1,
		}
	}

	rf.currentState = follower
	//rf.VotedFor = -1
	rf.hasReceiveRpc = false
	rf.persist()
	//reset timer
	rf.cancelFunc()
	rf.electionContext, rf.cancelFunc = context.WithCancel(context.Background())
	go func(ctx context.Context) {rf.followerElectionTimeout(ctx)}(rf.electionContext)
}

func (rf *Raft) changeToCandidate(){
	DPrintf("raft %s change to candidate ", rf.who())
	rf.currentState = candidate
	rf.CurrentTerm += 1
	rf.VotedFor = rf.me
	rf.receivedVotes = make(map[int]bool)
	rf.receivedVotes[rf.me] = true
	rf.persist()
	// reset timer
	rf.cancelFunc()
	rf.electionContext, rf.cancelFunc = context.WithCancel(context.Background())
	go func(ctx context.Context) {rf.candidateElectionTimeout(ctx)}(rf.electionContext)
	rf.sendAllRequestVote()
}

func (rf *Raft) changeToLeader() {
	DPrintf("raft %s change to leader ", rf.who())
	rf.currentState = leader

	for index, _ := range rf.peers{
		if index == rf.me{
			rf.matchIndex[index] = rf.getLastLogIndex()
			rf.nextIndex[index] = rf.matchIndex[index] + 1
			continue
		}

		rf.nextIndex[index] = rf.getLastLogIndex() + 1
		rf.matchIndex[index] = 0
		rf.idToAppending[index] = false
	}

	rf.sendAllAppendEntries(true)
	//reset timer
	rf.cancelFunc()
	rf.electionContext, rf.cancelFunc = context.WithCancel(context.Background())
	rf.hasSendHeartbeat = false
	go func(ctx context.Context) {rf.leaderElectionTimeout(ctx)}(rf.electionContext)
}

func (rf *Raft) majority() int {
	return len(rf.peers) / 2 + 1
}

func (rf *Raft) sendAllRequestVote(){
	for index, peer := range rf.peers{
		if index == rf.me{
			continue
		}
		go func(term uint64, id int, lastLogTerm uint64, lastLogIndex uint64, myPeer *labrpc.ClientEnd, index int){
			args := RequestVoteArgs{}
			args.Term = term
			args.CandidateId = id
			args.LastLogTerm = lastLogTerm
			args.LastLogIndex = lastLogIndex
			reply := RequestVoteReply{}
			ok := myPeer.Call("Raft.RequestVote", &args, &reply)
			//DPrintf("got request vote rpc ok %v", ok)
			if ok{
				rf.mu.Lock()
				defer rf.mu.Unlock()

				DPrintf("raft %s got vote reply %v from %d", rf.who(), reply, index)
				if reply.VoteGranted{
					if rf.currentState != candidate{
						return
					}

					rf.receivedVotes[index] = true
					if len(rf.receivedVotes) >= rf.majority(){
						// change to leader
						rf.changeToLeader()
					}
				}else{
					if reply.Term > rf.CurrentTerm {
						rf.CurrentTerm = reply.Term
						rf.VotedFor = -1
						rf.changeToFollower()
					}
				}
			}
		}(rf.CurrentTerm, rf.me, rf.getLastLogTerm(), rf.getLastLogIndex(), peer, index)
	}
}

func (rf *Raft) sendAllAppendEntries(isHeartbeat bool){
	rf.hasSendHeartbeat = true
	self := rf.me
	rf.matchIndex[self] = rf.getLastLogIndex()
	rf.nextIndex[self] = rf.matchIndex[self] + 1
	for index, peer := range rf.peers{
		if index == self{
			continue
		}
		rf.sendSingleAppendEntries(peer, index, isHeartbeat)
	}
}

func(rf *Raft) sendSingleAppendEntries(peer *labrpc.ClientEnd, index int, isHeartbeat bool){
	if isHeartbeat && rf.idToAppending[index]{
		return
	}

	rf.idToAppending[index] = true
	var log []Log
	nextIndex := rf.nextIndex[index]
	lastLogIndex := rf.getLastLogIndex()

	var prevLogIndex uint64
	var prevLogTerm uint64
	if lastLogIndex >= nextIndex{
		prevLogIndex = rf.getPrevLogIndex(nextIndex)
		if prevLogIndex < rf.lastSnapshotIndex{
			//install snapshot
		}else{
			prevLogTerm = rf.getLogTermAtIndex(prevLogIndex)
			log = make([]Log, lastLogIndex - nextIndex + 1)
			copy(log, rf.Logs[nextIndex - 1 - rf.lastSnapshotIndex: lastLogIndex - rf.lastSnapshotIndex])
		}
	}else{
		if !isHeartbeat{
			//if not heartbeat must have update
			return
		}
		log = make([]Log, 0, 0)
		prevLogIndex = lastLogIndex
		prevLogTerm = rf.getLastLogTerm()
	}

	DPrintf("raft %s sendSingleAppendEntries to raft %v isHeartbeat %v, Logs %v lastLogIndex %v nextIndex %v",
		rf.who(), index, isHeartbeat, log, lastLogIndex, nextIndex)
	go func(myPeer *labrpc.ClientEnd, index int, term uint64,
		prevLogIndex uint64, prevLogTerm uint64, log []Log, leaderCommit uint64){
		rf.realSendAppendEntries(myPeer, index, term, prevLogIndex, prevLogTerm, log, leaderCommit)
	}(peer, index, rf.CurrentTerm,
		prevLogIndex, prevLogTerm, log, rf.commitIndex)
}

func (rf *Raft) realSendAppendEntries(peer *labrpc.ClientEnd, index int, term uint64,
	prevLogIndex uint64, prevLogTerm uint64, log []Log, leaderCommit uint64){

	args := AppendEntriesArgs{}
	args.Term = term
	args.PrevLogIndex = prevLogIndex
	args.PrevLogTerm = prevLogTerm
	args.Entries = log
	args.LeaderCommit = leaderCommit
	reply := AppendEntriesReply{}
	reply.XLen = -1
	reply.XTerm = 0
	reply.XIndex = 0

	ok := peer.Call("Raft.AppendEntries", &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok{
		if reply.Term > rf.CurrentTerm {
			rf.CurrentTerm = reply.Term
			rf.changeToFollower()
			return
		}

		//state already changed , ignore
		if rf.currentState != leader{
			return
		}

		// need adjust nextIndex
		if reply.Success{
			newNextIndex := prevLogIndex + uint64(len(log)) + 1
			if newNextIndex > rf.nextIndex[index] || rf.matchIndex[index] == 0{
				rf.nextIndex[index] = prevLogIndex + uint64(len(log)) + 1
				rf.matchIndex[index] = rf.nextIndex[index] - 1
				DPrintf("raft %s increase follower %d next index to %d, match index to %d",
					rf.who(), index, rf.nextIndex[index], rf.matchIndex[index])

				// update leader commit index
				if rf.commitIndex < rf.matchIndex[index]{
					//find a property N
					matchIndexes := make([]uint64, 0, len(rf.matchIndex))

					for _, N := range rf.matchIndex{
						matchIndexes = append(matchIndexes, N)
					}

					sort.Slice(matchIndexes, func(i, j int) bool {return matchIndexes[i]< matchIndexes[j]})

					to := matchIndexes[rf.majority() - 1]
					if rf.commitIndex < to{
						rf.commitLogs(to)
					}
				}
			}
		}else{
			//adjust next index
			if reply.XLen != -1{
				//the follower len not match
				rf.nextIndex[index] = uint64(reply.XLen + 1)
			}else{
				//index match, check xterm
				if reply.XTerm != 0{
					pivot := reply.XIndex
					if pivot <= rf.lastSnapshotIndex || rf.getLogTermAtIndex(pivot - 1) != reply.XTerm{
						rf.nextIndex[index] = pivot
					}else{
						i := pivot + 1
						for i < rf.nextIndex[index]{
							if rf.getLogTermAtIndex(i - 1) != reply.XTerm{
								rf.nextIndex[index] = i
								break
							}
							i += 1
						}
					}
				}else{
					if rf.nextIndex[index] > 1{
						rf.nextIndex[index] -= 1
					}
				}
				DPrintf("raft %s adjust peer %d next index to %d, from reply %v",
					rf.who(), index, rf.nextIndex[index], reply)
			}
			rf.sendSingleAppendEntries(peer, index, false)
		}
	}
}


func (rf *Raft) who() string {
	printLogs := rf.Logs
	if len(printLogs) > 10{
		printLogs = printLogs[len(printLogs) - 10:]
	}

	return fmt.Sprintf("(%d, %d, Logs(%v) %v, commitIndex %v snapshot(%v %v) ",
		rf.me, rf.CurrentTerm, len(rf.Logs), printLogs, rf.commitIndex,
		rf.lastSnapshotIndex, rf.lastSnapshotTerm)
}

func (rf *Raft) getLastLogTerm() uint64{
	num := len(rf.Logs)
	if num > 0{
		return rf.Logs[num - 1].Term
	}else{
		return rf.lastSnapshotTerm
	}
}

func (rf *Raft) getLogTermAtIndex(index uint64) uint64{
	if index <= rf.lastSnapshotIndex{
		return rf.lastSnapshotTerm
	}else{
		return rf.Logs[index - rf.lastSnapshotIndex - 1].Term
	}
}

func (rf *Raft) getLastLogIndex() uint64{
	return uint64(len(rf.Logs)) + rf.lastSnapshotIndex
}

func (rf *Raft) getPrevLogIndex(pivotIndex uint64) uint64 {
	if pivotIndex == 0{
		return 0
	}else{
		return pivotIndex - 1
	}
}

func (rf *Raft) getPrevLogTerm(pivotIndex uint64) uint64{
	if pivotIndex == 0{
		return 0
	}else{
		if pivotIndex - 1 > rf.lastSnapshotIndex{
			return rf.Logs[pivotIndex - 1 - rf.lastSnapshotIndex - 1].Term
		}else{
			return rf.lastSnapshotTerm
		}
	}
}

func (rf *Raft) commitLogs(to uint64) {
	from := rf.commitIndex
	DPrintf("raft %s commitLogs to %d from %d", rf.who(), to, from)
	var applyMsg ApplyMsg
	if from <= to{
		//add index
		index := from + 1
		for index <= to{
			command := rf.Logs[index - 1].Command
			//go func(index uint64, command interface{}){
				applyMsg = ApplyMsg{
					true,
					command,
					int(index),
					false,
					make([]byte, 0),
					-1,
					-1,
				}
				rf.applyCh <- applyMsg
			//}(index, command)

			index += 1
		}
	}else{
		//remove index
		panic("commit index will not decrease")
		//index := to + 1
		//for index <= from{
		//	command := rf.Logs[index - 1].Command
		//	//go func(index uint64, command interface{}){
		//		applyMsg = ApplyMsg{false, command, int(index)}
		//		rf.applyCh <- applyMsg
		//	//}(index, rf.Logs[index-1].Command)
		//
		//	index += 1
		//}
	}

	rf.commitIndex = to

}

func (rf *Raft) setLogAtIndex(index uint64, log Log) {
	if index > rf.lastSnapshotIndex{
		rf.Logs[index - rf.lastSnapshotIndex - 1] = log
	}
}

func (rf *Raft) deleteLogsAfterIndex(index uint64) {
	if index <= rf.lastSnapshotIndex{
		rf.Logs = make([]Log, 0, 0)
	}else{
		rf.Logs = rf.Logs[:(index - rf.lastSnapshotIndex)]
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
	rf.currentState = follower
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.applyCh = applyCh
	rf.CurrentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastSnapshotIndex = 0

	rf.nextIndex = make(map[int]uint64)
	rf.matchIndex = make(map[int]uint64)
	rf.idToAppending = make(map[int]bool)

	rf.electionContext, rf.cancelFunc = context.WithCancel(context.Background())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.changeToFollower()
	// start ticker goroutine to start elections
	// go rf.ticker()

	DPrintf("create raft peer %s", rf.who())

	return rf
}

