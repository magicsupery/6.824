package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = false

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
	Command string // Get Put or Append
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
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Key: args.Key,
		Command: "Get",
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader{
		reply.Err = "NotLeader"
	}else{
		//wait for the command to commit
		waitChan := make(chan interface{})
		kv.indexToWaitChannel.Store(index, waitChan)

		result := <- waitChan
		reply.Value = result.(string)
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Key: args.Key,
		Value: args.Value,
		Command: args.Op,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader{
		reply.Err = "NotLeader"
	}else{
		//wait for the command to commit
		waitChan := make(chan interface{})
		DPrintf("server %s store waitchan with index %d", kv.who(), index)
		kv.indexToWaitChannel.Store(index, waitChan)

		<- waitChan
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
				index, op := command.CommandIndex, command.Command.(Op)

				if waitChan, ok := kv.indexToWaitChannel.Load(index); ok{
					if op.Command == "Get"{
						if val, ok := kv.keyToValue[op.Key]; ok{
							waitChan.(chan interface{}) <- val
						}else{
							waitChan.(chan interface{}) <- ""
						}
					}else if op.Command == "Put"{
						kv.keyToValue[op.Key] = op.Value
						waitChan.(chan interface{}) <- ""
					}else if op.Command == "Append"{
						if val, ok := kv.keyToValue[op.Key]; ok{
							kv.keyToValue[op.Key] = val + op.Value
						}else{
							kv.keyToValue[op.Key] = op.Value
						}

						waitChan.(chan interface{}) <- ""
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

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.closed = make(chan int)
	kv.keyToValue = make(map[string]string)
	kv.watchCommit()

	return kv
}
