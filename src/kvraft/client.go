package kvraft

import (
	"6.824/labrpc"
	"fmt"
	"github.com/google/uuid"
	"sync"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mutex sync.RWMutex
	leader int
	sendIndex int
	opIndex int
	id string
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leader = -1
	ck.sendIndex = 0
	ck.opIndex = 1
	ck.id = uuid.New().String()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	sendIndex := ck.getSendIndex()

	args := GetArgs{
		Key: key,
		ClientId: ck.id,
		OpIndex: ck.opIndex,
	}

	DPrintf("client %s send get %s to %d with arg %v ", ck.who(), key, sendIndex, args)
	reply := GetReply{
		Err: "",
	}

	ck.opIndex += 1

	ok := false
	for !ok{
		ok = ck.servers[sendIndex].Call("KVServer.Get", &args, &reply)
	}

	for reply.Err != ""{
		if reply.Err == ErrWrongLeader || reply.Err == ErrLeaderChanged{
			sendIndex = ck.adjustSendIndex(sendIndex)
		}

		reply.Err = ""
		ok = false
		for !ok{
			ok = ck.servers[sendIndex].Call("KVServer.Get", &args, &reply)
		}
	}


	ck.setLeaderIndex(sendIndex)

	DPrintf("client %s send get %s to %d with arg %v end", ck.who(), key, sendIndex, args)
	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	sendIndex := ck.getSendIndex()

	args := PutAppendArgs{
		Key: key,
		Value: value,
		Op: op,
		Id: uuid.New().String(),
		PrevIndex: 0,
		ClientId: ck.id,
		OpIndex: ck.opIndex,
	}

	DPrintf("client %s send putappend %s to %d with arg %v ", ck.who(), key, sendIndex, args)

	ck.opIndex += 1
	reply := PutAppendReply{
		Err: "",
	}
	ok := false
	for !ok{
		ok = ck.servers[sendIndex].Call("KVServer.PutAppend", &args, &reply)
	}

	for reply.Err != ""{
		if reply.Err == ErrWrongLeader || reply.Err == ErrLeaderChanged{
			sendIndex = ck.adjustSendIndex(sendIndex)
		}

		reply.Err = ""
		ok = false
		for !ok{
			ok = ck.servers[sendIndex].Call("KVServer.PutAppend", &args, &reply)
		}
	}

	ck.setLeaderIndex(sendIndex)

	DPrintf("client %s send putappend %s to %d with arg %v end", ck.who(), key, sendIndex, args)
	return
}

func (ck *Clerk) Put(key string, value string) {
	DPrintf("send  put %s", key)
	ck.PutAppend(key, value, "Put")

	DPrintf("send  put %s end", key)
}
func (ck *Clerk) Append(key string, value string) {
	DPrintf("send  append %s", key)
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) getSendIndex() int {
	ck.mutex.Lock()
	defer ck.mutex.Unlock()
	if ck.leader != -1{
		return ck.leader
	}else{
		ck.sendIndex = (ck.sendIndex + 1) % len(ck.servers)
		return ck.sendIndex
	}
}


func (ck *Clerk) adjustSendIndex(i int) int {
	ck.mutex.Lock()
	defer ck.mutex.Unlock()
	if ck.leader != -1{
		return ck.leader
	}else{
		return (i + 1) % len(ck.servers)
	}

}

func (ck *Clerk) setLeaderIndex(i int) {
	ck.mutex.Lock()
	defer ck.mutex.Unlock()
	ck.leader = i
}

func (ck *Clerk) who() string {
	return fmt.Sprintf("(%s)", ck.id)
}
