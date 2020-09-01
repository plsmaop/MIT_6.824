package kvraft

import (
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"sync"
	"time"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu            sync.RWMutex
	id            string
	currentLeader int
	seqNumber     int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

const (
	expFallbackWaitTime time.Duration = 2
)

func (ck *Clerk) getSeqNum() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	ck.seqNumber++
	return ck.seqNumber
}

func (ck *Clerk) setCurLeader(leader int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	if leader < 0 || leader >= len(ck.servers) {
		leader = int(nrand()) % len(ck.servers)
	}

	log.Printf("Set leader to %v", leader)
	ck.currentLeader = leader
}

func (ck *Clerk) getCurLeader() int {
	ck.mu.RLock()
	defer ck.mu.RUnlock()

	log.Printf("Get leader to %v", ck.currentLeader)
	return ck.currentLeader
}

//
// MakeClerk is the constructor of Clerk
//
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		servers:       servers,
		currentLeader: int(nrand()) % len(servers),
		id:            fmt.Sprintf("%v", nrand()),
		seqNumber:     0,
	}
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) sendGet(args *GetArgs) GetReply {
	reply := GetReply{}
	leader := ck.getCurLeader()
	waitTime := expFallbackWaitTime

	DPrintf("send ID(%v:%v): %v to %v", args.ClientID, args.SeqNum, args.Key, ck.servers[leader])
	ok := ck.servers[leader].Call("KVServer.Get", args, &reply)

	for !ok || reply.Err == ErrWrongLeader || reply.Err == ErrFail {
		args.Time = time.Now().UnixNano()
		if !ok || reply.Err == ErrFail {
			time.Sleep(waitTime * time.Nanosecond)
			waitTime *= waitTime
		}

		DPrintf("send ID(%v:%v): %v", args.ClientID, args.SeqNum, args.Key)
		reply = GetReply{}

		if reply.Err == ErrWrongLeader {
			leader = (leader + 1) % len(ck.servers)
		}

		ok = ck.servers[leader].Call("KVServer.Get", args, &reply)
	}

	ck.setCurLeader(leader)
	return reply
}

func (ck *Clerk) get(args *GetArgs) string {
	reply := ck.sendGet(args)

	switch reply.Err {
	case OK:
		DPrintf("GOTTTT: %v", reply.Value)
		return reply.Value
	case ErrNoKey:
		return ""
	case ErrDuplicate:
		log.Printf("Request Processed, discard")
		return ""
	default:
		log.Printf("No valid err type: %v", reply.Err)
		return ""
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
	args := GetArgs{
		Key:      key,
		Time:     time.Now().UnixNano(),
		ClientID: ck.id,
		SeqNum:   ck.getSeqNum(),
	}

	return ck.get(&args)
}

func (ck *Clerk) sendPutAppend(args *PutAppendArgs) PutAppendReply {
	reply := PutAppendReply{}
	leader := ck.getCurLeader()
	waitTime := expFallbackWaitTime

	DPrintf("send ID(%v:%v): %v to %v", args.ClientID, args.SeqNum, args.Key, ck.servers[leader])
	ok := ck.servers[leader].Call("KVServer.PutAppend", args, &reply)

	for !ok || reply.Err == ErrWrongLeader || reply.Err == ErrFail {
		args.Time = time.Now().UnixNano()
		if !ok || reply.Err == ErrFail {
			time.Sleep(waitTime * time.Nanosecond)
			waitTime *= waitTime
		}

		DPrintf("send ID(%v:%v): %v: %v", args.ClientID, args.SeqNum, args.Key, args.Value)
		reply = PutAppendReply{}

		if reply.Err == ErrWrongLeader {
			leader = (leader + 1) % len(ck.servers)
		}

		ok = ck.servers[leader].Call("KVServer.PutAppend", args, &reply)
	}

	ck.setCurLeader(leader)
	return reply
}

func (ck *Clerk) putAppend(args *PutAppendArgs) {
	reply := ck.sendPutAppend(args)

	switch reply.Err {
	case OK:
		return
	case ErrDuplicate:
		log.Printf("Request Processed, discard")
		return
	default:
		log.Printf("Invalid Reply, drop")
		return
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
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Time:     time.Now().UnixNano(),
		Op:       op,
		ClientID: ck.id,
		SeqNum:   ck.getSeqNum(),
	}

	ck.putAppend(&args)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, putType)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, appendType)
}
