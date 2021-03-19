package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"fmt"
	_ "github.com/google/uuid"
	"log"
	"sync"
	"sync/atomic"
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
	closed             chan int
	indexToWaitChannel sync.Map
	KeyToValue         map[string]string
	ClientToOpIndex    map[string]int
	CommitIndex        int
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

	//TODO 这里有一个实现问题，其实应该用opid把waitchan先放进去
	//否则可能raft的applychan上来，这里的waitchan还没有放到map里（极端情况，因为发生在不同协程）
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
				if command.SnapshotValid{
					if kv.rf.CondInstallSnapshot(command.SnapshotTerm, command.SnapshotIndex, command.Snapshot){
						kv.readSnapshot(command.Snapshot)
						kv.CommitIndex = command.SnapshotIndex
						DPrintf("server %s set commit index to snapshot index %d", kv.who(), kv.CommitIndex)
					}
				}else{
					message := command.Command.(raft.ServiceMessage)
					if message.MessageType == "op"{
						index := command.CommandIndex
						if index != kv.CommitIndex + 1{
							panic(fmt.Sprintf("server %s got index %d , but current commit index is %d",
							kv.who(), index, kv.CommitIndex))
						}

						kv.CommitIndex = index
						op := message.Obj.(Op)

						waitChan, hasChan:= kv.indexToWaitChannel.Load(index)

						if op.Command == "Get"{
							if val, ok := kv.KeyToValue[op.Key]; ok{
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
							if _, ok := kv.ClientToOpIndex[clientId]; !ok{
								kv.ClientToOpIndex[clientId] = 0
							}

							if kv.ClientToOpIndex[clientId] < op.OpIndex{
								kv.ClientToOpIndex[clientId] = op.OpIndex
								kv.KeyToValue[op.Key] = op.Value
							}

							DPrintf("server %s put [%s] = %s", kv.who(), op.Key, op.Value)
							if hasChan {
								waitChan.(chan OpResult) <- OpResult{true, ""}
							}
						}else if op.Command == "Append"{

							clientId := op.ClientId
							if _, ok := kv.ClientToOpIndex[clientId]; !ok{
								kv.ClientToOpIndex[clientId] = 0
							}

							if kv.ClientToOpIndex[clientId] < op.OpIndex{
								kv.ClientToOpIndex[clientId] = op.OpIndex
								if val, ok := kv.KeyToValue[op.Key]; ok{
									kv.KeyToValue[op.Key] = val + op.Value
								}else{
									kv.KeyToValue[op.Key] = op.Value
								}
							}

							DPrintf("server %s append [%s] = %s", kv.who(), op.Key, kv.KeyToValue[op.Key])
							if hasChan {
								waitChan.(chan OpResult) <- OpResult{true, ""}
							}
						}

						if kv.maxraftstate != -1 && kv.rf.Persister.RaftStateSize() > kv.maxraftstate{
							kv.rf.Snapshot(index, kv.persist())
						}

					}else if message.MessageType == "leaderChange"{
						leaderChange := message.Obj.(raft.LeaderChange)
						from := leaderChange.CommitIndex + 1

						DPrintf("server %s got leader change %v, from %d, logindex %d",
							kv.who(), leaderChange, from, leaderChange.LogIndex)
						for from <= leaderChange.LogIndex{
							if waitChan, ok := kv.indexToWaitChannel.Load(from); ok{
								DPrintf("server %s send change to chan %v", kv.who(), waitChan)
								// need delete the old chan
								kv.indexToWaitChannel.Delete(from)
								waitChan.(chan OpResult) <- OpResult{false, ErrLeaderChanged}
							}else{
								DPrintf("server %s send change to non chan %d", kv.who(), from)
							}
							from += 1
						}
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

func (kv *KVServer) readPersist(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}
	DPrintf("server %s got persist snapshot data size %d", kv.who(), len(snapshot))
	r1 := bytes.NewBuffer(snapshot)
	d1 := labgob.NewDecoder(r1)
	var data[]byte
	var lastSnapshotIndex uint64
	var lastSnapshotTerm uint64
	if d1.Decode(&data) != nil ||
		d1.Decode(&lastSnapshotIndex) != nil ||
		d1.Decode(&lastSnapshotTerm) != nil{
		panic(fmt.Sprintf("server %s read persist snapshot error", kv.who()))
	}else{
		if kv.readSnapshot(data){
			kv.CommitIndex = int(lastSnapshotIndex)
		}
	}
}

func (kv *KVServer)readSnapshot(snapshot []byte) bool{
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return true
	}
	DPrintf("server %s got snapshot data size %d", kv.who(), len(snapshot))

	r2 := bytes.NewBuffer(snapshot)
	d2 := labgob.NewDecoder(r2)
	var keyToValue map[string]string
	var clientToOpIndex map[string]int
	if d2.Decode(&keyToValue) != nil ||
		d2.Decode(&clientToOpIndex) != nil{
		panic(fmt.Sprintf("server %s read snapshot error", kv.who()))
	}else{
		kv.KeyToValue = keyToValue
		kv.ClientToOpIndex = clientToOpIndex
		DPrintf("server %s restore commit index to %d, kv to %v", kv.who(), kv.CommitIndex, kv.KeyToValue)
		return true
	}
}

func (kv *KVServer) persist() []byte{
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.KeyToValue)
	e.Encode(kv.ClientToOpIndex)
	return w.Bytes()
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

	kv.CommitIndex = 0
	kv.KeyToValue = make(map[string]string)
	kv.ClientToOpIndex = make(map[string]int)
	// You may need initialization code here.

	if kv.maxraftstate != -1{
		kv.readPersist(persister.ReadSnapshot())
	}
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.closed = make(chan int)

	kv.watchCommit()

	DPrintf("create server %s success", kv.who())
	return kv
}
