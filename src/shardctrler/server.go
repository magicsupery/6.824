package shardctrler

import (
	"6.824/raft"
	"fmt"
	"github.com/mohae/deepcopy"
	"log"
	"sort"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type OpResult struct{
	Ret bool
	Err string
	Value interface{}
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	// Your definitions here.
	closed             chan int
	indexToWaitChannel sync.Map
	ClientToOpIndex    map[string]int
	CommitIndex        int
}


type Op struct {
	// Your data here.
	Command string
	CommandArgs interface{}
	ClientId string
	OpIndex int
}


func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.

	DPrintf("%s Join args %v", sc.who(), args)

	op := Op{
		Command: "Join",
		CommandArgs: args.Servers,
		ClientId: args.ClientId,
		OpIndex: args.OpIndex,
	}

	message := raft.ServiceMessage{"op", op}

	index, _, isLeader:= sc.rf.Start(message)
	if !isLeader{
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}else{
		//wait for the command to commit
		waitChan := make(chan OpResult)
		sc.indexToWaitChannel.Store(index, waitChan)
		DPrintf("%s store chan %v to index %d ", sc.who(), waitChan, index)
		result := <- waitChan
		if !result.Ret{
			reply.WrongLeader = true
			reply.Err = ErrLeaderChanged
		}else{
			reply.WrongLeader = false
		}

		return
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.

	DPrintf("%s Leave args %v", sc.who(), args)

	op := Op{
		Command: "Leave",
		CommandArgs: args.GIDs,
		ClientId: args.ClientId,
		OpIndex: args.OpIndex,
	}

	message := raft.ServiceMessage{"op", op}

	index, _, isLeader:= sc.rf.Start(message)
	if !isLeader{
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}else{
		//wait for the command to commit
		waitChan := make(chan OpResult)
		sc.indexToWaitChannel.Store(index, waitChan)
		DPrintf("%s store chan %v to index %d ", sc.who(), waitChan, index)
		result := <- waitChan
		if !result.Ret{
			reply.WrongLeader = true
			reply.Err = ErrLeaderChanged
		}else{
			reply.WrongLeader = false
		}

		return
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.

	DPrintf("%s Move args %v", sc.who(), args)

	op := Op{
		Command: "Move",
		CommandArgs: *args,
		ClientId: args.ClientId,
		OpIndex: args.OpIndex,
	}

	message := raft.ServiceMessage{"op", op}

	index, _, isLeader:= sc.rf.Start(message)
	if !isLeader{
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}else{
		//wait for the command to commit
		waitChan := make(chan OpResult)
		sc.indexToWaitChannel.Store(index, waitChan)
		DPrintf("%s store chan %v to index %d ", sc.who(), waitChan, index)
		result := <- waitChan
		if !result.Ret{
			reply.WrongLeader = true
			reply.Err = ErrLeaderChanged
		}else{
			reply.WrongLeader = false
		}

		return
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.

	DPrintf("%s Query args %v", sc.who(), args)

	op := Op{
		Command: "Query",
		CommandArgs: args.Num,
	}

	message := raft.ServiceMessage{"op", op}

	index, _, isLeader:= sc.rf.Start(message)
	if !isLeader{
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}else{
		//wait for the command to commit
		waitChan := make(chan OpResult)
		sc.indexToWaitChannel.Store(index, waitChan)
		DPrintf("%s store chan %v to index %d ", sc.who(), waitChan, index)

		result := <- waitChan
		if !result.Ret{
			reply.WrongLeader = true
			reply.Err = ErrLeaderChanged
		}else{
			reply.WrongLeader = false
			reply.Config = result.Value.(Config)
		}

		return
	}
}


//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	close(sc.closed)
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	sc.configs[0].Num = 0

	labgob.Register(Op{})
	labgob.Register(raft.ServiceMessage{})
	labgob.Register(raft.LeaderChange{})
	labgob.Register(MoveArgs{})
	labgob.Register(map[int][]string{})

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.

	sc.CommitIndex = 0
	sc.ClientToOpIndex = make(map[string]int)
	sc.closed = make(chan int)

	sc.watchCommit()

	return sc
}

func (sc *ShardCtrler) who() string {
	return fmt.Sprintf("sc (%d)", sc.me)
}

func (sc *ShardCtrler) isDupOp(clientId string, opIndex int) bool {
	if _, ok := sc.ClientToOpIndex[clientId]; !ok{
		sc.ClientToOpIndex[clientId] = 0
	}

	if sc.ClientToOpIndex[clientId] < opIndex{
		sc.ClientToOpIndex[clientId] = opIndex
		return false
	}

	return true
}

func (sc *ShardCtrler) watchCommit() {
	go func(){
		for{
			select{
			case command := <- sc.applyCh:
				DPrintf("%s got command %v ", sc.who(), command)
				message := command.Command.(raft.ServiceMessage)
				if message.MessageType == "op"{
					index := command.CommandIndex
					if index != sc.CommitIndex + 1{
						panic(fmt.Sprintf("%s got index %d , but current commit index is %d",
							sc.who(), index, sc.CommitIndex))
					}


					sc.CommitIndex = index
					op := message.Obj.(Op)

					waitChan, hasChan:= sc.indexToWaitChannel.Load(index)

					if op.Command == "Join"{
						if !sc.isDupOp(op.ClientId, op.OpIndex){
							oldConfig := sc.configs[len(sc.configs) - 1]
							newConfig := Config{
								Num : oldConfig.Num + 1,
								Shards: deepcopy.Copy(oldConfig.Shards).([10]int),
								Groups: deepcopy.Copy(oldConfig.Groups).(map[int][]string),
							}

							servers := op.CommandArgs.(map[int][]string)

							for gid, names := range servers{
								if _, ok := newConfig.Groups[gid];!ok{
									newConfig.Groups[gid] = names
								}else{
									newConfig.Groups[gid] = append(newConfig.Groups[gid], names...)
								}
							}

							sc.balanceShards(&newConfig)
							sc.configs = append(sc.configs, newConfig)
						}

						if hasChan {
							waitChan.(chan OpResult) <- OpResult{true, "", nil}
						}
					}else if op.Command == "Leave"{
						if !sc.isDupOp(op.ClientId, op.OpIndex){
							oldConfig := sc.configs[len(sc.configs) - 1]
							newConfig := Config{
								Num : oldConfig.Num + 1,
								Shards: deepcopy.Copy(oldConfig.Shards).([10]int),
								Groups: deepcopy.Copy(oldConfig.Groups).(map[int][]string),
							}

							gids := op.CommandArgs.([]int)

							for _, gid:= range gids{
								if _, ok := newConfig.Groups[gid]; ok{
									delete(newConfig.Groups, gid)
								}
							}

							sc.balanceShards(&newConfig)
							sc.configs = append(sc.configs, newConfig)
						}

						if hasChan {
							waitChan.(chan OpResult) <- OpResult{true, "", nil}
						}
					}else if op.Command == "Move" {
						if !sc.isDupOp(op.ClientId, op.OpIndex){
							oldConfig := sc.configs[len(sc.configs) - 1]
							newConfig := Config{
								Num : oldConfig.Num + 1,
								Shards: deepcopy.Copy(oldConfig.Shards).([10]int),
								Groups: deepcopy.Copy(oldConfig.Groups).(map[int][]string),
							}

							moveArgs := op.CommandArgs.(MoveArgs)
							newConfig.Shards[moveArgs.Shard] = moveArgs.GID

							sc.configs = append(sc.configs, newConfig)
						}

						if hasChan {
							waitChan.(chan OpResult) <- OpResult{true, "", nil}
						}
					} else if op.Command == "Query" {

						num := op.CommandArgs.(int)

						retConfig := sc.configs[len(sc.configs) - 1]
						if num == -1 || num >= retConfig.Num {

						}else{
							offset := retConfig.Num - num
							retConfig = sc.configs[len(sc.configs) - 1 - offset]

						}
						if hasChan {
							waitChan.(chan OpResult) <- OpResult{true, "", retConfig}
						}
					}
			}else if message.MessageType == "leaderChange"{
					leaderChange := message.Obj.(raft.LeaderChange)
					from := leaderChange.CommitIndex + 1

					DPrintf("%s got leader change %v, from %d, logindex %d",
						sc.who(), leaderChange, from, leaderChange.LogIndex)

					for from <= leaderChange.LogIndex{
						if waitChan, ok := sc.indexToWaitChannel.Load(from); ok{
							DPrintf("%s send change to chan %v", sc.who(), waitChan)
							// need delete the old chan
							sc.indexToWaitChannel.Delete(from)
							waitChan.(chan OpResult) <- OpResult{false, ErrLeaderChanged, nil}
						}else{
							DPrintf("%s send change to non chan %d", sc.who(), from)
						}
						from += 1
					}
				}
			case <- sc.closed:
				DPrintf("%s watchCommit closed", sc.who())
			}
		}
	}()
}

func (sc *ShardCtrler) balanceShards(newConfig *Config){
	//sort all gids
	keys := make([]int, 0, len(newConfig.Groups))
	for gid, _ := range newConfig.Groups{
		keys = append(keys, gid)
	}

	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	gidLen := len(keys)
	index := 0
	//remap sharding to new gid
	if gidLen == 0{
		for index < len(newConfig.Shards){
			newConfig.Shards[index] = 0
			index += 1
		}
	}else{
		for index < len(newConfig.Shards){
			newConfig.Shards[index] = keys[index % gidLen]
			index += 1
		}
	}
}

