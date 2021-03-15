package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	_ "github.com/google/uuid"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Command string // Get Put Append Notify
	ClientId string
	OpIndex int
	OpId string
}

type OpResult struct{
	Ret bool
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int
	// snapshot if log grows this big

	// Your definitions here.
	closed       chan int
	indexToWaitChannel sync.Map
	keyToValue	 map[string]string
	clientToOpIndex map[string]int
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("server %s Get args %v", kv.who(), args)
	op := Op{
		Key: args.Key,
		Command: "Get",
	}

	message := raft.ServiceMessage{"op", op}
	index, _, isLeader:= kv.rf.Start(message)
	if !isLeader{
		reply.Err = ErrWrongLeader
		return
	}else {
		//wait for the command to commit
		waitChan := make(chan OpResult)
		kv.indexToWaitChannel.Store(index, waitChan)

		DPrintf("server %s store chan %v to index %d ", kv.who(), waitChan, index)
		result := <- waitChan
		kv.indexToWaitChannel.Delete(index)
		DPrintf("server %s got get result %v from index %d", kv.who(), result, index)
		if result.Ret {
			reply.Value = result.Value
		} else {
			reply.Err = Err(result.Value)
		}

		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	DPrintf("server %s putappend args %v", kv.who(), args)

	op := Op{
		Key: args.Key,
		Value: args.Value,
		Command: args.Op,
		ClientId: args.ClientId,
		OpIndex: args.OpIndex,
	}

	message := raft.ServiceMessage{"op", op}

	index, _, isLeader:= kv.rf.Start(message)
	if !isLeader{
		reply.Err = ErrWrongLeader
		return
	}else{
		//wait for the command to commit
		waitChan := make(chan OpResult)
		kv.indexToWaitChannel.Store(index, waitChan)
		DPrintf("server %s store chan %v to index %d ", kv.who(), waitChan, index)
		result := <- waitChan
		kv.indexToWaitChannel.Delete(index)
		DPrintf("server %s got putappend result %v from index %d", kv.who(), result, index)
		if !result.Ret{
			reply.Err = Err(result.Value)
		}

		return
	}

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.closed)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) watchCommit() {
	go func(){
		for{
			select{
			case command := <- kv.applyCh:
				DPrintf("server %s got command %v ", kv.who(), command)
				message := command.Command.(raft.ServiceMessage)
				if message.MessageType == "op"{
					index := command.CommandIndex
					op := message.Obj.(Op)

					waitChan, hasChan:= kv.indexToWaitChannel.Load(index)

					if op.Command == "Get"{
						if val, ok := kv.keyToValue[op.Key]; ok{
							if hasChan {
								waitChan.(chan OpResult) <- OpResult{true, val}
							}
						}else{
							if hasChan {
								waitChan.(chan OpResult) <- OpResult{true, ""}
							}
						}
					}else if op.Command == "Put"{
						clientId := op.ClientId
						if _, ok := kv.clientToOpIndex[clientId]; !ok{
							kv.clientToOpIndex[clientId] = 0
						}

						if kv.clientToOpIndex[clientId] < op.OpIndex{
							kv.clientToOpIndex[clientId] = op.OpIndex
							kv.keyToValue[op.Key] = op.Value
						}

						if hasChan {
							waitChan.(chan OpResult) <- OpResult{true, ""}
						}
					}else if op.Command == "Append"{
						clientId := op.ClientId
						if _, ok := kv.clientToOpIndex[clientId]; !ok{
							kv.clientToOpIndex[clientId] = 0
						}

						if kv.clientToOpIndex[clientId] < op.OpIndex{
							kv.clientToOpIndex[clientId] = op.OpIndex
							if val, ok := kv.keyToValue[op.Key]; ok{
								kv.keyToValue[op.Key] = val + op.Value
							}else{
								kv.keyToValue[op.Key] = op.Value
							}
						}

						if hasChan {
							waitChan.(chan OpResult) <- OpResult{true, ""}
						}
					}

				}else if message.MessageType == "leaderChange"{
					leaderChange := message.Obj.(raft.LeaderChange)
					from := leaderChange.CommitIndex + 1

					DPrintf("server %s got leader change %v, from %d, logindex %d",
						kv.who(), leaderChange, from, leaderChange.LogIndex)
					for from <= leaderChange.LogIndex{
						if waitChan, ok := kv.indexToWaitChannel.Load(from); ok{
							DPrintf("server %s send change to chan %v", kv.who(), waitChan)
							waitChan.(chan OpResult) <- OpResult{false, ErrLeaderChanged}
						}else{
							DPrintf("server %s send change to non chan %d", kv.who(), from)
						}
						from += 1
					}
				}

			case <- kv.closed:
				DPrintf("server %s watchCommit closed", kv.who())
				return
			}
		}
	}()
}

func (kv *KVServer) who() string {
	return fmt.Sprintf("(%d)", kv.me)
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(raft.ServiceMessage{})
	labgob.Register(raft.LeaderChange{})
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.closed = make(chan int)
	kv.keyToValue = make(map[string]string)
	kv.clientToOpIndex = make(map[string]int)
	kv.watchCommit()

	return kv
}
