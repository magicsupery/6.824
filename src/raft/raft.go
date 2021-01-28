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
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"


type NodeState int
const (
	follower  NodeState = iota
	candidate
	leader
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
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
	currentTerm  uint64
	votedFor     int
	log          []string
	logTerm 	 []uint64

	commitIndex uint64
	lastApplied uint64

	nextIndex  map[int]uint64
	matchIndex map[int]uint64

	hasReceiveRpc bool // if received Rpc between election timeout
	receivedVotes int

	electionContext  context.Context
	cancelFunc       context.CancelFunc
	hasSendHeartbeat bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return int(rf.currentTerm), rf.currentState == leader
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
	DPrintf("raft %s got request vote", rf.who())
	rf.hasReceiveRpc = true

	//TODO up-to-date
	if rf.currentTerm > args.Term{
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	}else{
		if rf.currentTerm < args.Term{
			rf.currentTerm = args.Term
			rf.changeToFollower()
		}

		if rf.votedFor == -1 &&
			((args.LastLogTerm > rf.getLastLogTerm()) ||
				(args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex())){
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			rf.votedFor = args.CandidateId
		}else{
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
		}
	}
}

type AppendEntriesArgs struct {
	Term uint64
	LeaderId int
	PrevLogIndex uint64
	PrevLogTerm uint64
	Entries []string
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term uint64
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("raft %s got append entries", rf.who())
	rf.hasReceiveRpc = true
	if rf.currentTerm > args.Term{
		reply.Success = false
		reply.Term = rf.currentTerm
	}else{
		if rf.currentTerm < args.Term{
			rf.currentTerm = args.Term
			rf.changeToFollower()
		}
		reply.Success = true
		reply.Term = rf.currentTerm

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
	isLeader := true

	// Your code here (2B).


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
// confusing debug output. any goroutine with a long-running electiontimeout
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

func (rf *Raft) followerElectionTimeout(ctx context.Context) {
	//get random expires timeout
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
			_, _ = DPrintf("follower election time done")

	}

}

func (rf *Raft) candidateElectionTimeout(ctx context.Context){
	select{
		case <- time.After(rf.getElectionTimeout()):
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.changeToCandidate()
		case <- ctx.Done():
			_, _ = DPrintf("candidate election time done")
	}
}

func (rf *Raft) leaderElectionTimeout(ctx context.Context) {
	select{
	case <- time.After(rf.getElectionTimeout() / 3):
		rf.mu.Lock()
		defer rf.mu.Unlock()

		//DPrintf("raft %s leader election timeout", rf.who())
		if !rf.hasSendHeartbeat{
			rf.sendAllAppendEntries()
		}

		//continue
		rf.hasSendHeartbeat = false
		rf.cancelFunc()
		rf.electionContext, rf.cancelFunc = context.WithCancel(context.Background())
		go func(ctx context.Context){rf.leaderElectionTimeout(ctx)}(rf.electionContext)
	case <- ctx.Done():
		_, _ = DPrintf("leader election time done")
	}
}
//changeTo function need lock outside by caller
func (rf *Raft) changeToFollower(){
	DPrintf("raft %s change to follower ", rf.who())
	rf.currentState = follower
	rf.votedFor = -1
	rf.hasReceiveRpc = false

	//reset timer
	rf.cancelFunc()
	rf.electionContext, rf.cancelFunc = context.WithCancel(context.Background())
	go func(ctx context.Context) {rf.followerElectionTimeout(ctx)}(rf.electionContext)
}

func (rf *Raft) changeToCandidate(){
	DPrintf("raft %s change to candidate ", rf.who())
	rf.currentState = candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.receivedVotes = 1
	// reset timer
	rf.cancelFunc()
	rf.electionContext, rf.cancelFunc = context.WithCancel(context.Background())
	go func(ctx context.Context) {rf.candidateElectionTimeout(ctx)}(rf.electionContext)
	rf.sendAllRequestVote()
}

func (rf *Raft) changeToLeader() {
	DPrintf("raft %s change to leader ", rf.who())
	rf.currentState = leader
	rf.sendAllAppendEntries()

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
		go func(term uint64, id int, lastLogTerm uint64, lastLogIndex uint64, myPeer *labrpc.ClientEnd){
			args := RequestVoteArgs{}
			args.Term = term
			args.CandidateId = id
			reply := RequestVoteReply{}
			ok := myPeer.Call("Raft.RequestVote", &args, &reply)
			//DPrintf("got request vote rpc ok %v", ok)
			if ok{
				rf.mu.Lock()
				defer rf.mu.Unlock()


				DPrintf("reply VoteGranted %v", reply.VoteGranted)
				if reply.VoteGranted{
					if rf.currentState != candidate{
						return
					}

					rf.receivedVotes += 1
					if rf.receivedVotes >= rf.majority(){
						// change to leader
						rf.changeToLeader()
					}
				}else{
					if reply.Term > rf.currentTerm{
						rf.currentTerm = reply.Term
						rf.changeToFollower()
					}
				}
			}
		}(rf.currentTerm, rf.me, rf.getLastLogTerm(), rf.getLastLogIndex(), peer)
	}
}

func (rf *Raft) sendAllAppendEntries(){
	rf.hasSendHeartbeat = true
	for index, peer := range rf.peers{
		if index == rf.me{
			continue
		}

		go func(term uint64, myPeer *labrpc.ClientEnd){
			args := AppendEntriesArgs{}
			args.Term = term
			reply := AppendEntriesReply{}
			ok := myPeer.Call("Raft.AppendEntries", &args, &reply)
			if ok{
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.currentTerm{
					rf.currentTerm = reply.Term
					rf.changeToFollower()
					return
				}

				if !reply.Success{
					//resend
				}
			}
		}(rf.currentTerm, peer)
	}
}

func (rf *Raft) who() string {
	return fmt.Sprintf("(%d, %d)", rf.me, rf.currentTerm)
}

func (rf *Raft) getLastLogTerm() uint64{
	num := len(rf.log)
	if num > 0{
		return rf.logTerm[num - 1]
	}else{
		return 0
	}
}

func (rf *Raft) getLastLogIndex() uint64{
	return uint64(len(rf.log))
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
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make(map[int]uint64)
	rf.matchIndex = make(map[int]uint64)

	rf.electionContext, rf.cancelFunc = context.WithCancel(context.Background())
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.changeToFollower()

	DPrintf("create raft peer %s", rf.who())
	return rf
}

