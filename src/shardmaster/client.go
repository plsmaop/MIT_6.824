package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"sync/atomic"
	"time"

	"../labrpc"
)

const (
	reqTimeout   = 2 * time.Second
	receiver     = "ShardMaster"
	queryRPCName = receiver + ".Query"
	joinRPCName  = receiver + ".Join"
	leaveRPCName = receiver + ".Leave"
	movePCName   = receiver + ".Move"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
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

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		servers:       servers,
		currentLeader: int64(nrand()) % int64(len(servers)),
		id:            fmt.Sprintf("%v", nrand()),
		seqNumber:     0,
	}
	// Your code here.
	return ck
}

func (ck *Clerk) send(leader int64, rpcName string, args interface{}, reply interface{}) bool {
	doneChan := make(chan bool)
	go func() {
		doneChan <- ck.servers[leader].Call(rpcName, args, reply)
		close(doneChan)
	}()

	select {
	case <-time.After(reqTimeout):
		return false
	case ok := <-doneChan:
		return ok
	}
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{
		Header: Header{
			ClientID: ck.id,
			SeqNum:   int64(ck.getSeqNum()),
		},
		Num: num,
	}
	// Your code here.
	for {
		// try each known server.
		curLeader := ck.getCurLeader()
		for i := 0; i < len(ck.servers); i++ {
			srv := (curLeader + int64(i)) % int64(len(ck.servers))

			var reply QueryReply
			ok := ck.send(int64(srv), queryRPCName, args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				ck.setCurLeader(int64(srv))
				return reply.Config
			}
		}
		// time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{
		Header: Header{
			ClientID: ck.id,
			SeqNum:   int64(ck.getSeqNum()),
		},
		Servers: servers,
	}
	// Your code here.

	for {
		// try each known server.
		curLeader := ck.getCurLeader()
		for i := 0; i < len(ck.servers); i++ {
			srv := (curLeader + int64(i)) % int64(len(ck.servers))

			var reply JoinReply
			ok := ck.send(int64(srv), joinRPCName, args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				ck.setCurLeader(int64(srv))
				return
			}
		}
		// time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{
		Header: Header{
			ClientID: ck.id,
			SeqNum:   int64(ck.getSeqNum()),
		},
		GIDs: gids,
	}
	// Your code here.

	for {
		// try each known server.
		curLeader := ck.getCurLeader()
		for i := 0; i < len(ck.servers); i++ {
			srv := (curLeader + int64(i)) % int64(len(ck.servers))

			var reply LeaveReply
			ok := ck.send(int64(srv), leaveRPCName, args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				ck.setCurLeader(int64(srv))
				return
			}
		}
		// time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{
		Header: Header{
			ClientID: ck.id,
			SeqNum:   int64(ck.getSeqNum()),
		},
		ShardGIDPair: ShardGIDPair{
			Shard: shard,
			GID:   gid,
		},
	}
	// Your code here.

	for {
		// try each known server.
		curLeader := ck.getCurLeader()
		for i := 0; i < len(ck.servers); i++ {
			srv := (curLeader + int64(i)) % int64(len(ck.servers))

			var reply MoveReply
			ok := ck.send(int64(srv), movePCName, args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				ck.setCurLeader(int64(srv))
				return
			}
		}

		// time.Sleep(100 * time.Millisecond)
	}
}
