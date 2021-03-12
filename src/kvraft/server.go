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
	Id string
	OpIndex int
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
	indexToWaitChannel map[int]interface{}
	keyToValue	 map[string]string
	commitIndex int
	clientToOpIndex map[string]int
	clientOpIndexToLogIndex map[string]map[int]int
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()


	DPrintf("server %s Get args %v", kv.who(), args)
	if _, ok := kv.clientToOpIndex[args.ClientId]; !ok{
		kv.clientToOpIndex[args.ClientId] = 0
		kv.clientOpIndexToLogIndex[args.ClientId] = make(map[int]int)
	}

	//dup ops
	if args.OpIndex <= kv.clientToOpIndex[args.ClientId]{
		//find the op binding logIndex in raft
		logIndex := kv.clientOpIndexToLogIndex[args.ClientId][args.OpIndex]
		//already success
		if kv.commitIndex > logIndex{
			reply.Value = kv.keyToValue[args.Key]
			kv.mu.Unlock()
			return
		}else{
			waitChan := make(chan OpResult)
			kv.indexToWaitChannel[logIndex] = waitChan
			kv.mu.Unlock()

			result := <- waitChan
			if result.Ret{
				reply.Value = result.Value
			}else{
				reply.Err = Err(result.Value)
			}

			kv.mu.Lock()
			delete(kv.clientOpIndexToLogIndex[args.ClientId], args.OpIndex)
			kv.mu.Unlock()
			return
		}
	}else{

		op := Op{
			Key: args.Key,
			Command: "Get",
		}

		message := raft.ServiceMessage{"op", op}
		index, _, isLeader:= kv.rf.Start(message)
		if !isLeader{
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
			return
		}else{
			//wait for the command to commit
			waitChan := make(chan OpResult)
			kv.indexToWaitChannel[index] =  waitChan
			kv.clientOpIndexToLogIndex[args.ClientId][args.OpIndex] = index
			kv.mu.Unlock()

			result := <- waitChan
			if result.Ret{
				reply.Value = result.Value
			}else{
				reply.Err = Err(result.Value)
			}

			kv.mu.Lock()
			delete(kv.clientOpIndexToLogIndex[args.ClientId], args.OpIndex)
			kv.mu.Unlock()
			return
		}
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()

	DPrintf("server %s putappend args %v", kv.who(), args)

	if _, ok := kv.clientToOpIndex[args.ClientId]; !ok{
		kv.clientToOpIndex[args.ClientId] = 0
		kv.clientOpIndexToLogIndex[args.ClientId] = make(map[int]int)
	}

	//dup ops
	if args.OpIndex <= kv.clientToOpIndex[args.ClientId]{
		DPrintf("can not happend")
		//find the op binding logIndex in raft
		logIndex := kv.clientOpIndexToLogIndex[args.ClientId][args.OpIndex]
		//already success
		if kv.commitIndex > logIndex{
			kv.mu.Unlock()
			return
		}else{
			waitChan := make(chan OpResult)
			kv.indexToWaitChannel[logIndex] = waitChan
			kv.mu.Unlock()

			result := <- waitChan
			DPrintf("server %s putappend args %v got result %v", kv.who(), args, result)
			if !result.Ret{
				reply.Err = Err(result.Value)
			}

			kv.mu.Lock()
			delete(kv.clientOpIndexToLogIndex[args.ClientId], args.OpIndex)
			kv.mu.Unlock()
			return
		}
	}else{

		op := Op{
			Key: args.Key,
			Value: args.Value,
			Command: args.Op,
			Id: args.Id,
		}

		message := raft.ServiceMessage{"op", op}

		index, _, isLeader:= kv.rf.Start(message)
		if !isLeader{
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
			return
		}else{
			//wait for the command to commit
			waitChan := make(chan OpResult)
			kv.indexToWaitChannel[index] =  waitChan
			kv.clientOpIndexToLogIndex[args.ClientId][args.OpIndex] = index

			DPrintf("server %s putappend args %v wait for chan %p", kv.who(), args, &waitChan)
			kv.mu.Unlock()

			result := <- waitChan

			DPrintf("server %s putappend args %v got result %v", kv.who(), args, result)
			if !result.Ret{
				reply.Err = Err(result.Value)
			}

			kv.mu.Lock()
			delete(kv.clientOpIndexToLogIndex[args.ClientId], args.OpIndex)
			kv.mu.Unlock()
			return
		}
	}

	// check if retry already in leader's log
	//if args.PrevIndex != 0{
	//	ret, command := kv.rf.GetLog(args.PrevIndex)
	//	if !ret{
	//		kv.doPutAppend(args, reply)
	//	}else{
	//		serviceMessage := command.(raft.ServiceMessage)
	//		if serviceMessage.MessageType == "op"{
	//			op := serviceMessage.Obj.(Op)
	//			if op.Id == args.Id{
	//				//dup operation find
	//				kv.mu.Lock()
	//
	//				if args.PrevIndex <= kv.commitIndex{
	//					kv.mu.Unlock()
	//					return
	//				}else{
	//					waitChan := make(chan OpResult)
	//					DPrintf("server %s store waitchan with index %d", kv.who(), args.PrevIndex)
	//					kv.indexToWaitChannel.Store(args.PrevIndex, waitChan)
	//					kv.mu.Unlock()
	//
	//					result := <- waitChan
	//					if !result.Ret{
	//						reply.Err = Err(result.Value)
	//					}
	//
	//					return
	//				}
	//			}else{
	//				kv.doPutAppend(args, reply)
	//			}
	//		}else{
	//			kv.doPutAppend(args, reply)
	//		}
	//	}
	//}else{
	//	kv.doPutAppend(args, reply)
	//}

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

					kv.mu.Lock()
					kv.commitIndex = index
					if waitChan, ok := kv.indexToWaitChannel[index]; ok{
						DPrintf("server %s got command %v to channel %p", kv.who(), command, &waitChan)
						if op.Command == "Get"{
							if val, ok := kv.keyToValue[op.Key]; ok{
								kv.mu.Unlock()
								waitChan.(chan OpResult) <- OpResult{true, val}
							}else{
								kv.mu.Unlock()
								waitChan.(chan OpResult) <- OpResult{true, ""}
							}
						}else if op.Command == "Put"{
							kv.keyToValue[op.Key] = op.Value
							kv.mu.Unlock()
							waitChan.(chan OpResult) <- OpResult{true, ""}
						}else if op.Command == "Append"{
							if val, ok := kv.keyToValue[op.Key]; ok{
								kv.keyToValue[op.Key] = val + op.Value
							}else{
								kv.keyToValue[op.Key] = op.Value
							}

							kv.mu.Unlock()
							waitChan.(chan OpResult) <- OpResult{true, ""}
						}
					}else{
						kv.mu.Unlock()
					}
				}else if message.MessageType == "leaderChange"{
					leaderChange := message.Obj.(raft.LeaderChange)
					from := leaderChange.CommitIndex + 1
					for from <= leaderChange.LogIndex{
						kv.mu.Lock()
						if waitChan, ok := kv.indexToWaitChannel[int(from)]; ok{
							waitChan.(chan OpResult) <- OpResult{false, ErrLeaderChanged}
						}
						from += 1
						kv.mu.Unlock()
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
	kv.indexToWaitChannel = make(map[int]interface {})
	kv.clientToOpIndex = make(map[string]int)
	kv.clientOpIndexToLogIndex = make(map[string]map[int]int)
	kv.watchCommit()

	return kv
}
