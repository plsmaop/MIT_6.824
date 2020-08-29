package kvraft

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"sync"
	"time"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu            sync.RWMutex
	currentLeader int
	id            int64
	serialNumber  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (ck *Clerk) getTxID() string {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	ck.serialNumber++
	return fmt.Sprintf("%v:%v", ck.id, ck.serialNumber)
}

func (ck *Clerk) setCurLeader(leader int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	if leader < 0 || leader >= len(ck.servers) {
		leader = int(nrand()) % len(ck.servers)
	}
	ck.currentLeader = leader
}

func (ck *Clerk) getCurLeader() int {
	ck.mu.RLock()
	defer ck.mu.RUnlock()

	return ck.currentLeader
}

//
// MakeClerk is the constructor of Clerk
//
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		servers:       servers,
		currentLeader: int(nrand()) % len(servers),
		id:            nrand(),
		serialNumber:  0,
	}
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) get(args *GetArgs, reply *GetReply) {
	reply = &GetReply{}
	ok := ck.servers[ck.getCurLeader()].Call("KVServer.Get", args, reply)
	for !ok {
		reply = &GetReply{}
		ok = ck.servers[ck.getCurLeader()].Call("KVServer.Get", args, reply)
	}
}

//
// Get fetch the current value for a key.
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
	id := ck.getTxID()
	args := GetArgs{
		Key:  key,
		Time: time.Now().UnixNano(),
		ID:   id,
	}
	reply := GetReply{}
	ck.get(&args, &reply)

	switch reply.Err {
	case OK:
		return reply.Value
	case ErrNoKey:
		return ""
	case ErrWrongLeader:
		ck.setCurLeader(reply.LeaderID)
		ck.get(&args, &reply)
	}

	return ""
}

func (ck *Clerk) putAppend(args *PutAppendArgs, reply *PutAppendReply) {
	reply = &PutAppendReply{}
	ok := ck.servers[ck.getCurLeader()].Call("KVServer.PutAppend", args, reply)
	for !ok {
		reply = &PutAppendReply{}
		ok = ck.servers[ck.getCurLeader()].Call("KVServer.PutAppend", args, reply)
	}
}

//
// PutAppend shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op opType) {
	// You will have to modify this function.
	id := ck.getTxID()
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Time:  time.Now().UnixNano(),
		Op:    op,
		ID:    id,
	}

	reply := PutAppendReply{}
	ck.putAppend(&args, &reply)

	switch reply.Err {
	case OK:
		return
	case ErrWrongLeader:
		ck.setCurLeader(reply.LeaderID)
		ck.putAppend(&args, &reply)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, putType)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, appendType)
}
