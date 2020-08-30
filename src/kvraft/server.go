package kvraft

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	raftTimeout = time.Minute
	workerNum   = 10
)

type KVStore struct {
	mu      sync.RWMutex
	kvTable map[string]interface{}
}

func (kvs *KVStore) Get(k string) (interface{}, bool) {
	kvs.mu.RLock()
	defer kvs.mu.RUnlock()

	v, ok := kvs.kvTable[k]
	return v, ok
}

func (kvs *KVStore) Put(k string, v interface{}) {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()

	kvs.kvTable[k] = v
}

func (kvs *KVStore) Delete(k string) bool {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()

	if _, ok := kvs.kvTable[k]; !ok {
		return false
	}

	delete(kvs.kvTable, k)
	return true
}

func (kvs *KVStore) AtomicOp(k string, op func(interface{}) interface{}) bool {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()

	kvs.kvTable[k] = op(kvs.kvTable[k])
	return true
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type     opType
	Key      string
	Value    interface{}
	ClientID string
	SeqNum   int
}

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store       KVStore
	jobTable    KVStore
	clientTable map[string]int
}

type job struct {
	ind  int
	op   Op
	done chan Op
}

func (kv *KVServer) Get(args Args, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	opType := args.GetOp()
	cID := args.GetClientID()
	seqNum := args.GetSeqNum()

	if kv.clientTable[cID] >= seqNum {
		// applied
		reply.Err = ErrDuplicate
		reply.Time = time.Now().UnixNano()
		kv.mu.Unlock()
		return
	}

	ind, _, ok := kv.rf.Start(Op{
		Type:     getType,
		Key:      args.GetKey(),
		ClientID: cID,
		SeqNum:   seqNum,
	})

	if !ok {
		reply.Err = ErrWrongLeader
		reply.Time = time.Now().UnixNano()
		kv.mu.Unlock()
		return
	}

	kv.clientTable[cID] = seqNum
	kv.mu.Unlock()

	done := make(chan Op)
	kv.jobTable.AtomicOp(fmt.Sprintf("%v", ind), kv.appendJobWrapper(job{
		ind: ind,
		op: Op{
			Type:     opType,
			ClientID: cID,
			SeqNum:   seqNum,
		},
		done: done,
	}))

	opDone := <-done
	reply.Time = time.Now().UnixNano()
	if opDone.Type != opType || opDone.ClientID != cID || opDone.SeqNum != seqNum {
		reply.Err = ErrFail
		kv.printf("%v:%v failed", cID, seqNum)
	} else {
		if opDone.Value != nil {
			reply.Err = OK
			v, _ := opDone.Value.(string)
			reply.Value = v
		} else {
			reply.Err = ErrNoKey
			reply.Value = ""
		}

		kv.printf("%v:%v finished", cID, seqNum)
	}
}

func (kv *KVServer) PutAppend(args Args, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	opType := args.GetOp()
	cID := args.GetClientID()
	seqNum := args.GetSeqNum()

	if kv.clientTable[cID] >= seqNum {
		// applied
		reply.Err = ErrDuplicate
		reply.Time = time.Now().UnixNano()
		kv.mu.Unlock()
		return
	}

	ind, _, ok := kv.rf.Start(Op{
		Type:     opType,
		Key:      args.GetKey(),
		Value:    args.GetValue(),
		ClientID: cID,
		SeqNum:   seqNum,
	})

	if !ok {
		reply.Err = ErrWrongLeader
		reply.Time = time.Now().UnixNano()
		kv.mu.Unlock()
		return
	}

	kv.clientTable[cID] = seqNum
	kv.mu.Unlock()

	done := make(chan Op)
	kv.jobTable.AtomicOp(fmt.Sprintf("%v", ind), kv.appendJobWrapper(job{
		ind: ind,
		op: Op{
			Type:     opType,
			ClientID: cID,
			SeqNum:   seqNum,
		},
		done: done,
	}))

	opDone := <-done
	reply.Time = time.Now().UnixNano()
	if opDone.Type != opType || opDone.ClientID != cID || opDone.SeqNum != seqNum {
		reply.Err = ErrFail
		kv.printf("%v:%v failed", cID, seqNum)
	} else {
		reply.Err = OK
		kv.printf("%v:%v finished", cID, seqNum)
	}
}

func (kv *KVServer) apply(msg raft.ApplyMsg) {
	if !msg.CommandValid {
		return
	}

	cmd, ok := msg.Command.(Op)
	if !ok {
		log.Printf("Invalid cmd: %v, discard\n", msg.Command)
	}

	cmdInd := fmt.Sprintf("%v", msg.CommandIndex)
	entry, ok := kv.jobTable.Get(cmdInd)
	if !ok {
		return
	}

	jobs, _ := entry.([]job)
	switch cmd.Type {
	case getType:
		v, _ := kv.store.Get(cmd.Key)
		for _, job := range jobs {
			cmd.Value = v
			job.done <- cmd
			close(job.done)
		}
		kv.jobTable.Delete(cmdInd)
	case putType:
		kv.store.Put(cmd.Key, cmd.Value)
		for _, job := range jobs {
			job.done <- cmd
			close(job.done)
		}
		kv.jobTable.Delete(cmdInd)
	case appendType:
		v, _ := cmd.Value.(string)
		kv.store.AtomicOp(cmd.Key, kv.appendWrapper(v))
		for _, job := range jobs {
			job.done <- cmd
			close(job.done)
		}
		kv.jobTable.Delete(cmdInd)
	default:
		log.Printf("Invalid cmd type: %v, discard\n", cmd.Type)
	}
}

func (kv *KVServer) appendJobWrapper(j job) func(interface{}) interface{} {
	return func(v interface{}) interface{} {
		val, ok := v.([]job)
		if !ok {
			return []job{j}
		}

		return append(val, j)
	}
}

func (kv *KVServer) appendWrapper(s string) func(interface{}) interface{} {
	return func(v interface{}) interface{} {
		val, ok := v.(string)
		if !ok {
			return s
		}

		kv.printf("FFF: %v", val+s)
		return val + s
	}
}

func (kv *KVServer) isReqDuplicated(cID string, seqNum int) bool {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	return kv.clientTable[cID] >= seqNum
}

func (kv *KVServer) updateClientSeqNum(cID string, seqNum int) {
	kv.mu.Lock()
	kv.mu.Unlock()

	kv.clientTable[cID] = seqNum
}

func (kv *KVServer) startLoop() {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for !kv.killed() {
			time.Sleep(100 * time.Millisecond)
		}

		cancel()
	}()

	// apply
	for i := 0; i < workerNum; i++ {
		go func() {
			select {
			case <-ctx.Done():
				return
			case applyMsg := <-kv.applyCh:
				kv.printf("applyMsg: %v", applyMsg)
				kv.apply(applyMsg)
			}
		}()
	}
}

//
// for debug
//
func (kv *KVServer) printf(format string, a ...interface{}) {
	a = append([]interface{}{kv.me}, a...)
	DPrintf("%v: "+format, a...)
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
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
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

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.jobTable = KVStore{
		kvTable: make(map[string]interface{}),
	}

	kv.store = KVStore{
		kvTable: make(map[string]interface{}),
	}

	kv.clientTable = make(map[string]int)

	// You may need initialization code here.
	kv.startLoop()

	return kv
}
