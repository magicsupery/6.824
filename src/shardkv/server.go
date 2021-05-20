package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"fmt"
	"github.com/mohae/deepcopy"
	"log"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

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
	CommandObj interface{}
	ClientId string
	OpIndex int
	OpId string
}

type OpResult struct{
	Ret bool
	Value string
}

type KVMapState int

const (
	STABLE KVMapState = iota
	TRANSFER
)

type KVMap struct{
	Res map[string]string
	Status KVMapState
}

type QueryMsg struct {
	gid int
	num int
}

type ShardTransfer struct{
	Shard int
	Src int
	Dst int
	ConfigNum int
	KV KVMap
}
type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	mck          *shardctrler.Clerk
	servers []*labrpc.ClientEnd
	// Your definitions here.

	closed              chan int
	indexToWaitChannel  sync.Map
	ShardToKVMap        map[int]*KVMap
	ClientToOpIndex     map[string]int
	CommitIndex         int
	Config              shardctrler.Config
	configNum           int
	configReadyNum      int32
	shardToTransferInfo map[int]ShardTransfer
	gidToReadyNum       map[int]int
	readyChan           chan QueryMsg
	queryConfig         shardctrler.Config
	closedFetch         chan int
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
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
		if result.Ret {
			reply.Value = result.Value
			reply.Err = OK
		} else {
			reply.Err = Err(result.Value)
		}

		return
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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

		if !result.Ret{
			reply.Err = Err(result.Value)
		}else{
			reply.Err = OK
		}

		return
	}

}

func (kv *ShardKV) Transfer(args *TransferArgs, reply *TransferReply) {
	// Your code here.
	defer func() {
		DPrintf("server %s transfer args %v end", kv.who(), args)
	}()

	DPrintf("server %s transfer args %v", kv.who(), args)

	op := Op{
		Command: "Transfer",
		CommandObj: args.Obj,
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

		if !result.Ret{
			reply.Err = Err(result.Value)
		}else{
			reply.Err = OK
		}

		return
	}

}

func (kv *ShardKV) QueryReady(args *QueryReadyArgs, reply *QueryReadyReply) {
	// Your code here.
	//DPrintf("server %s query ready args %v", kv.who(), args)

	reply.Err = OK
	reply.Num = int(atomic.LoadInt32(&kv.configReadyNum))
}
//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.closed)
	close(kv.closedFetch)
}

func (kv *ShardKV) readPersist(snapshot []byte) {
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
		kv.readSnapshot(data)
		kv.CommitIndex = int(lastSnapshotIndex)
	}
}

func (kv *ShardKV) watchCommit() {
	go func(){
		for{
			select{
			case command := <- kv.applyCh:
				DPrintf("server %s got command %v ", kv.who(), command)
				if command.SnapshotValid{
					if kv.rf.CondInstallSnapshot(command.SnapshotTerm, command.SnapshotIndex, command.Snapshot){
						kv.readSnapshot(command.Snapshot)
						from := kv.CommitIndex
						kv.CommitIndex = command.SnapshotIndex
						DPrintf("server %s set commit index to snapshot index %d from %d",
							kv.who(), kv.CommitIndex, from)
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

							ret := false
							value := ""
							if retMap, ok := kv.getSharedMapByKey(op.Key);ok {
								ret = true
								if val, ok := retMap[op.Key];ok{
									value = val
								}

								DPrintf("server %s get [%s] = %s", kv.who(), op.Key, value)
							}else{
								ret = false
								value = ErrWrongGroup
							}
							//DPrintf("server %s debug for map %v", kv.who(), kv.ShardToKVMap)
							if hasChan {
								waitChan.(chan OpResult) <- OpResult{ret, value}
							}
						}else if op.Command == "Put"{
							ret := false
							value := ""

							if retMap, ok := kv.getSharedMapByKey(op.Key);ok{
								ret = true
								if !kv.isDupOp(op.ClientId, op.OpIndex){
									DPrintf("server %s put [%s] = %s", kv.who(), op.Key, op.Value)
									retMap[op.Key] = op.Value
								}
							}else{
								ret = false
								value = ErrWrongGroup
							}

							if hasChan {
								waitChan.(chan OpResult) <- OpResult{ret, value}
							}
						}else if op.Command == "Append"{
							ret := false
							value := ""

							if retMap, ok := kv.getSharedMapByKey(op.Key);ok {
								ret = true
								if !kv.isDupOp(op.ClientId, op.OpIndex){
									if val, ok := retMap[op.Key]; ok{
										retMap[op.Key] = val + op.Value
									}else{
										retMap[op.Key] = op.Value
									}

									DPrintf("server %s append [%s] = %s", kv.who(), op.Key, retMap[op.Key])
								}

							}else{
								value = ErrWrongGroup
							}
							if hasChan {
								waitChan.(chan OpResult) <- OpResult{ret, value}
							}
						}else if op.Command == "Config"{
							config := op.CommandObj.(shardctrler.Config)
							DPrintf("server %s got config change %v ", kv.who(), config)
							if config.Num > int(atomic.LoadInt32(&kv.configReadyNum)) + 1{
								panic(fmt.Sprintf("server %s got config change num %d , but ready num is %d",
									kv.who(), config.Num, atomic.LoadInt32(&kv.configReadyNum)))
							}

							if config.Num < int(atomic.LoadInt32(&kv.configReadyNum)) + 1{
								panic(fmt.Sprintf("server %s got config change num %d , but ready num is %d ignore it",
									kv.who(), config.Num, atomic.LoadInt32(&kv.configReadyNum)))
							}

							kv.shardToTransferInfo = make(map[int]ShardTransfer)
							for shard, gid := range config.Shards{
								kvmap, ok := kv.ShardToKVMap[shard]
								if ok{
									if gid == kv.gid{
										//old have, new have
									}else{
										//old have, new not have
										//remove, set the state to TRANSFER, then send
										//delete(kv.ShardToKVMap, shard)
										kvmap.Status = TRANSFER
										kv.shardToTransferInfo[shard] = ShardTransfer{
											Shard: shard,
											Src: kv.gid,
											Dst: gid,
											ConfigNum: config.Num,
											KV: *(deepcopy.Copy(kvmap).(*KVMap)),
										}

										//DPrintf("server %s debug for map %v", kv.who(), kv.ShardToKVMap)
									}
								}else{
									if gid == kv.gid{
										//old not have, new have
										if config.Num == 1{
											//create
											kv.ShardToKVMap[shard] = &KVMap{
												Res : make(map[string]string),
												Status : STABLE,
											}
											DPrintf("server %s create shard %d", kv.who(), kv.gid)
										}else{
											//wait for receiving

											kv.shardToTransferInfo[shard] = ShardTransfer{
												Shard: shard,
												Src: kv.Config.Shards[shard],
												Dst: kv.gid,
												ConfigNum: config.Num,
											}
										}
									}else{
										//old not have, new not have
									}
								}
							}
							kv.Config = config

							if len(kv.shardToTransferInfo) == 0{
								atomic.AddInt32(&kv.configReadyNum, 1)
								DPrintf("server %s add config ready num to %d", kv.who(), atomic.LoadInt32(&kv.configReadyNum))
							}else{
								DPrintf("server %s got ShardTransferInfo %v", kv.who(), kv.shardToTransferInfo)
								for shard, info:= range kv.shardToTransferInfo{
									if info.Src != kv.gid{
										continue
									}
									DPrintf("server %s need send shard %d from %d to %d",
										kv.who(), shard, info.Src, info.Dst)

									kv.goSendTransfer(kv.shardToTransferInfo[shard], shard, info.Dst, true)
								}
							}

							DPrintf("server %s got config command end ", kv.who())
						}else if op.Command == "Transfer"{
							shardTransfer := op.CommandObj.(ShardTransfer)
							if shardTransfer.ConfigNum > kv.Config.Num{
								DPrintf(fmt.Sprintf("server %s got shard transfer %v , but cur config num is %d need sender retry",
									kv.who(), shardTransfer, kv.Config.Num))

								if hasChan{
									waitChan.(chan OpResult) <- OpResult{false, ErrWrongConfigNum}
								}
							}else if shardTransfer.ConfigNum < kv.Config.Num{
								// 这种情况相当于sender crash后，从新发送，但是receiver已经接收完毕，告诉sender即可
								DPrintf(fmt.Sprintf("server %s got shard transfer %v , but cur config num is %d tell sender already receive",
									kv.who(), shardTransfer, kv.Config.Num))
								if hasChan{
									waitChan.(chan OpResult) <- OpResult{true, ""}
								}
							}else{
								if _, ok := kv.shardToTransferInfo[shardTransfer.Shard];ok{
									delete(kv.shardToTransferInfo, shardTransfer.Shard)
									if shardTransfer.Src == kv.gid{
										delete(kv.ShardToKVMap, shardTransfer.Shard)
										DPrintf("server %s delete transfer shard %d", kv.who(), shardTransfer.Shard)
									}else if shardTransfer.Dst == kv.gid{
										shardTransfer.KV.Status = STABLE
										kvmap := deepcopy.Copy(shardTransfer.KV).(KVMap)
										kv.ShardToKVMap[shardTransfer.Shard] = &kvmap
										DPrintf("server %s create transfer shard %d", kv.who(), shardTransfer.Shard)
									}
									if len(kv.shardToTransferInfo) == 0{
										atomic.AddInt32(&kv.configReadyNum, 1)
										DPrintf("server %s add config ready num to %d after transfer", kv.who(), atomic.LoadInt32(&kv.configReadyNum))
									}
								}else{
									DPrintf("server %s got transfer shard %v, but not in the transfer map %v",
										kv.who(), shardTransfer, kv.shardToTransferInfo)
								}

								if hasChan {
									waitChan.(chan OpResult) <- OpResult{true, ""}
								}
							}

						}
						//else if op.Command == "QueryReady"{
						//	if hasChan{
						//		num := atomic.LoadInt32(&kv.configReadyNum)
						//		waitChan.(chan OpResult) <-
						//			OpResult{true, strconv.FormatInt(int64(num), 10)}
						//	}
						//}

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
								waitChan.(chan OpResult) <- OpResult{false, ErrWrongLeader}
							}else{
								DPrintf("server %s send change to non chan %d", kv.who(), from)
							}
							from += 1
						}
					}
				}
				DPrintf("server %s got command %v end", kv.who(), command)
			case <- kv.closed:
				DPrintf("server %s watchCommit closed", kv.who())
				return
			}
		}
	}()
}

func (kv *ShardKV) who() interface{} {
	return fmt.Sprintf("(%d, %d)", kv.gid, kv.me)
}

func (kv *ShardKV) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}
	DPrintf("server %s got snapshot data size %d", kv.who(), len(snapshot))

	r2 := bytes.NewBuffer(snapshot)
	d2 := labgob.NewDecoder(r2)
	var clientToOpIndex map[string]int
	var shardToKVMap map[int]*KVMap
	var shardToTransferInfo map[int]ShardTransfer
	var config shardctrler.Config
	var configReadyNum int32
	if d2.Decode(&clientToOpIndex) != nil ||
		d2.Decode(&shardToKVMap) != nil ||
		d2.Decode(&shardToTransferInfo) != nil ||
		d2.Decode(&config) != nil ||
		d2.Decode(&configReadyNum) != nil{
		panic(fmt.Sprintf("server %s read snapshot error", kv.who()))
	}else{
		kv.ClientToOpIndex = clientToOpIndex
		kv.ShardToKVMap = shardToKVMap
		kv.shardToTransferInfo = shardToTransferInfo
		kv.Config = config
		atomic.StoreInt32(&kv.configReadyNum, configReadyNum)
		DPrintf("server %s restore commit index to %d, configReadyNum to %d", kv.who(), kv.CommitIndex, configReadyNum)
	}
}

func (kv *ShardKV) persist() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.ClientToOpIndex)
	e.Encode(kv.ShardToKVMap)
	e.Encode(kv.shardToTransferInfo)
	e.Encode(kv.Config)
	e.Encode(atomic.LoadInt32(&kv.configReadyNum))
	return w.Bytes()
}

func (kv *ShardKV) isDupOp(clientId string, opIndex int) bool{

	if _, ok := kv.ClientToOpIndex[clientId]; !ok{
		kv.ClientToOpIndex[clientId] = 0
	}

	if kv.ClientToOpIndex[clientId] < opIndex{
		kv.ClientToOpIndex[clientId] = opIndex
		return false
	}

	return true

}

func (kv *ShardKV) getSharedMapByKey(key string) (map[string]string, bool){
	if retMap, ok := kv.ShardToKVMap[key2shard(key)];ok{
		if retMap.Status == STABLE{
			return retMap.Res, true
		}else{
			return nil, false
		}
	}
	return nil, false
}

func (kv *ShardKV) fetchConfig() {
	go func(){
		for {
			select {
			case <- time.After(time.Millisecond * 100):
				DPrintf("server %s timeout %p", kv.who(), &kv.readyChan)
				// current config changed ok?
				if kv.isCurConfReady(){
					newNum := int(atomic.LoadInt32(&kv.configReadyNum)) + 1
					if newNum < kv.configNum + 1{
						newNum = kv.configNum + 1
					}
					newConfig := kv.mck.Query(newNum)
					//replace
					if newConfig.Num == newNum{
						DPrintf("server %s config change from %d to %d", kv.who(), kv.configNum, kv.configNum + 1)
						op := Op{
							Command: "Config",
							CommandObj: newConfig,
						}

						message := raft.ServiceMessage{"op", op}
						kv.rf.Start(message)
						kv.configNum = newNum
						//old has

						DPrintf("server %s check gidToReadyNum %v for %d", kv.who(), kv.gidToReadyNum, kv.configNum)

						kv.gidToReadyNum = make(map[int]int)
						for gid, _ := range newConfig.Groups{
							kv.gidToReadyNum[gid] = 0
						}
						kv.gidToReadyNum[kv.gid] = 0

						DPrintf("server %s generate gidToReadyNum %v for %d", kv.who(), kv.gidToReadyNum, kv.configNum)
						kv.queryConfig = newConfig
					}else{
						kv.gidToReadyNum = make(map[int]int)
					}
				}else{
					DPrintf("server %s query ready num of %d for config %v", kv.who(), kv.configNum, kv.gidToReadyNum)
					//query other group the ready num
					for gid, num := range kv.gidToReadyNum{
						if num == kv.configNum{
							continue
						}

						if gid == kv.gid{
							kv.gidToReadyNum[gid] = int(atomic.LoadInt32(&kv.configReadyNum))
							continue
						}

						//query
						go func(gid int){
							args := QueryReadyArgs{
							}
							for{
								if servers, ok := kv.queryConfig.Groups[gid]; ok {
									// try each server for the shard.
									for si := 0; si < len(servers); si++ {
										srv := kv.make_end(servers[si])
										var reply QueryReadyReply
										ok := srv.Call("ShardKV.QueryReady", &args, &reply)
										if ok && reply.Err == OK {
											//transfer ok, tell raft
											kv.readyChan <- QueryMsg{gid, reply.Num}
											return
										}
									}
								}
							}
						}(gid)

					}
				}
			case msg := <- kv.readyChan:
				DPrintf("server %s update ready num for %d to %d", kv.who(), msg.gid, msg.num)
				if _, ok := kv.gidToReadyNum[msg.gid];ok{
					kv.gidToReadyNum[msg.gid] = msg.num
				}
			case <- kv.closedFetch:
				DPrintf("server %s fetchConfig closed", kv.who())
				return
			}
		}
	}()
}

func (kv *ShardKV) isCurConfReady() bool{
	for _, num := range kv.gidToReadyNum{
		if num < kv.configNum{
			return false
		}
	}

	return true
}

func (kv *ShardKV) goSendTransfer(obj ShardTransfer, shard int, dst int, notifySrc bool) {
	go func(config shardctrler.Config, shard int, dst int){
		args := TransferArgs{
			Obj : obj,
		}
		for{
			if servers, ok := config.Groups[dst]; ok {
				// try each server for the shard.
				for si := 0; si < len(servers); si++ {
					srv := kv.make_end(servers[si])
					var reply TransferReply
					ok := false
					for !ok || reply.Err != OK{
						ok = srv.Call("ShardKV.Transfer", &args, &reply)
						if reply.Err == ErrWrongLeader{
							break
						}else if reply.Err == OK{
							kv.goSendTransferForSelfGroup(obj, shard)
							return
						}
						time.Sleep(time.Millisecond * 100)
					}
				}
			}
		}
	}(kv.Config, shard, dst)
}

func (kv *ShardKV) goSendTransferForSelfGroup(obj ShardTransfer, shard int) {
	go func(shard int){
		args := TransferArgs{
			Obj : obj,
		}
		for {
			for si := 0; si < len(kv.servers); si++ {
				srv := kv.servers[si]
				var reply TransferReply
				ok := false
				for !ok || reply.Err != OK {
					ok = srv.Call("ShardKV.Transfer", &args, &reply)
					if reply.Err == ErrWrongLeader {
						break
					} else if reply.Err == OK {
						return
					}
					time.Sleep(time.Millisecond * 100)
				}
			}
		}
	}(shard)
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler. // // make_end(servername) turns a server name from a // Config.Groups[gid][i] into a labrpc.ClientEnd on which you can // send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(raft.ServiceMessage{})
	labgob.Register(raft.LeaderChange{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(ShardTransfer{})
	kv := new(ShardKV)
	kv.servers = servers
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.CommitIndex = 0
	kv.ShardToKVMap = make(map[int]*KVMap)
	kv.ClientToOpIndex = make(map[string]int)
	kv.Config = shardctrler.Config{
		Num : 0,
	}

	kv.closed = make(chan int)
	kv.closedFetch = make(chan int)
	kv.configNum = kv.Config.Num
	kv.configReadyNum = int32(kv.configNum)
	kv.gidToReadyNum = make(map[int]int)
	kv.readyChan = make(chan QueryMsg)
	if kv.maxraftstate != -1{
		kv.readPersist(persister.ReadSnapshot())
	}
	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	//kv.rf.SetID(kv.gid)

	for shard, info:= range kv.shardToTransferInfo{
		if info.Src != kv.gid{
			continue
		}
		DPrintf("server %s need send shard %d from %d to %d",
			kv.who(), shard, info.Src, info.Dst)

		kv.goSendTransfer(kv.shardToTransferInfo[shard], shard, info.Dst, true)
	}

	kv.watchCommit()
	kv.fetchConfig()

	DPrintf("create server %s success", kv.who())
	return kv
}
