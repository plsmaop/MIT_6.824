package kvraft

import (
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

const (
	reqTimeout = 2 * time.Second
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu            sync.RWMutex
	id            string
	currentLeader int64
	seqNumber     int64
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

func (ck *Clerk) getSeqNum() int64 {
	return atomic.AddInt64(&ck.seqNumber, 1)
}

func (ck *Clerk) setCurLeader(leader int64) {
	ck.printf("Set leader to %v", leader)
	atomic.StoreInt64(&ck.currentLeader, leader)
}

func (ck *Clerk) getCurLeader() int64 {
	return atomic.LoadInt64(&ck.currentLeader)
}

func (ck *Clerk) printf(format string, a ...interface{}) {
	if debug > 0 {
		a = append([]interface{}{ck.id}, a...)
		log.Printf("%v "+format, a...)
	}
}

//
// MakeClerk is the constructor of Clerk
//
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		servers:       servers,
		currentLeader: int64(nrand()) % int64(len(servers)),
		id:            fmt.Sprintf("%v", nrand()),
		seqNumber:     0,
	}
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) send(leader int64, rpcName string, args Args, reply Reply) bool {
	doneChan := make(chan bool)
	go func() {
		doneChan <- ck.servers[leader].Call(rpcName, args, reply)
	}()

	select {
	case <-time.After(reqTimeout):
		return false
	case ok := <-doneChan:
		return ok
	}
}

func (ck *Clerk) sendGet(args *GetArgs) GetReply {
	reply := GetReply{}
	leader := ck.getCurLeader()
	waitTime := expFallbackWaitTime

	ck.printf("send %v to %v", args, leader)
	ok := ck.send(leader, "KVServer.Get", args, &reply)
	ck.printf("received: %v : %v from %v", args, reply, leader)

	for !ok || reply.Err == ErrWrongLeader || reply.Err == ErrFail {

		if !ok {
			time.Sleep(waitTime * time.Nanosecond)
			waitTime *= waitTime
		}

		if reply.Err == ErrFail {
			ck.printf("failed : %v", args)
		}

		if reply.Err != ErrFail {
			leader = (leader + 1) % int64(len(ck.servers))
		}

		args.Time = time.Now().UnixNano()
		ck.printf("send %v to %v", args, leader)
		reply = GetReply{}

		ok = ck.send(leader, "KVServer.Get", args, &reply)
		ck.printf("received: %v : %v from %v", args, reply, leader)
	}

	ck.setCurLeader(leader)
	return reply
}

func (ck *Clerk) get(args *GetArgs) string {
	reply := ck.sendGet(args)

	switch reply.Err {
	case OK:
		ck.printf("GOTTTT: %v", reply.Value)
		return reply.Value
	case ErrNoKey:
		return ""
	case ErrDuplicate:
		ck.printf("Request Processed, discard")
		return ""
	default:
		ck.printf("No valid err type: %v", reply.Err)
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
		SeqNum:   int64(ck.getSeqNum()),
	}

	ck.printf("ck get: %v", args)
	return ck.get(&args)
}

func (ck *Clerk) sendPutAppend(args *PutAppendArgs) PutAppendReply {
	reply := PutAppendReply{}
	leader := ck.getCurLeader()
	waitTime := expFallbackWaitTime

	ck.printf("send %v to %v", args, leader)
	ok := ck.send(leader, "KVServer.PutAppend", args, &reply)
	ck.printf("received: %v : %v from %v", args, reply, leader)

	for !ok || reply.Err == ErrWrongLeader || reply.Err == ErrFail {
		if !ok {
			time.Sleep(waitTime * time.Nanosecond)
			waitTime *= waitTime
		}

		if reply.Err == ErrFail {
			ck.printf("failed : %v", args)
		}

		if reply.Err != ErrFail {
			leader = (leader + 1) % int64(len(ck.servers))
		}

		args.Time = time.Now().UnixNano()
		ck.printf("send %v to %v", args, leader)
		reply = PutAppendReply{}

		ok = ck.send(leader, "KVServer.PutAppend", args, &reply)
		ck.printf("received: %v : %v from %v", args, reply, leader)
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
		ck.printf("Request Processed, discard")
		return
	default:
		ck.printf("Invalid Reply, drop")
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

	ck.printf("ck put append: %v", args)
	ck.putAppend(&args)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, putType)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, appendType)
}
