package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"sync/atomic"
	"time"

	"../labrpc"
	"../shardmaster"
)

const (
	reqTimeout       = 2 * time.Second
	receiver         = "ShardKV"
	getRPCName       = receiver + ".Get"
	putAppendRPCName = receiver + ".PutAppend"
)

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	id            string
	currentLeader int64
	seqNumber     int64
}

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
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		id:        fmt.Sprintf("%v", nrand()),
		seqNumber: 0,
		sm:        shardmaster.MakeClerk(masters),
		make_end:  make_end,
	}
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) send(leader *labrpc.ClientEnd, rpcName string, args interface{}, reply interface{}) bool {
	doneChan := make(chan bool)
	go func() {
		doneChan <- leader.Call(rpcName, args, reply)
		close(doneChan)
	}()

	select {
	case <-time.After(reqTimeout):
		return false
	case ok := <-doneChan:
		return ok
	}
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Header: Header{
			ClientID: ck.id,
			SeqNum:   int64(ck.getSeqNum()),
		},
		Key: key,
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]

		curLeader := ck.getCurLeader()
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srvInd := (curLeader + int64(si)) % int64(len(servers))

				srv := ck.make_end(servers[srvInd])
				var reply GetReply
				ok := ck.send(srv, getRPCName, &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.setCurLeader(int64(si))
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					args.SeqNum = int64(ck.getSeqNum())
					curLeader = 0
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op opType) {
	args := PutAppendArgs{
		Header: Header{
			ClientID: ck.id,
			SeqNum:   int64(ck.getSeqNum()),
		},
		Key:   key,
		Value: value,
		Op:    op,
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]

		curLeader := ck.getCurLeader()
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srvInd := (curLeader + int64(si)) % int64(len(servers))

				srv := ck.make_end(servers[srvInd])
				var reply PutAppendReply
				ok := ck.send(srv, putAppendRPCName, &args, &reply)
				if ok && reply.Err == OK {
					ck.setCurLeader(int64(si))
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					args.SeqNum = int64(ck.getSeqNum())
					curLeader = 0
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, putType)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, appendType)
}
