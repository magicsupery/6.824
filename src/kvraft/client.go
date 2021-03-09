package kvraft

import (
	"6.824/labrpc"
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
	DPrintf("send get %s", key)
	// You will have to modify this function.
	sendIndex := ck.getSendIndex()

	args := GetArgs{
		Key: key,
	}
	reply := GetReply{
		Err: "",
	}

	ck.servers[sendIndex].Call("KVServer.Get", &args, &reply)
	for reply.Err == "NotLeader"{
		sendIndex = ck.adjustSendIndex(sendIndex)
		ck.servers[sendIndex].Call("KVServer.Get", &args, &reply)
	}

	ck.setLeaderIndex(sendIndex)
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
	}

	reply := PutAppendReply{
		Err: "",
	}

	ck.servers[sendIndex].Call("KVServer.PutAppend", &args, &reply)
	for reply.Err == "NotLeader"{
		sendIndex = ck.adjustSendIndex(sendIndex)
		reply.Err = ""
		ck.servers[sendIndex].Call("KVServer.PutAppend", &args, &reply)
		DPrintf("1 send index %d, reply err %s", sendIndex, reply.Err)
	}

	ck.setLeaderIndex(sendIndex)
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
