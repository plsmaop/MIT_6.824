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

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if debug > 0 {
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
	SeqNum   int64
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
	clientTable map[string]*client
	appliedInd  int64
}

type reqStatus string

const (
	working   reqStatus = "working"
	successed reqStatus = "successed"
	failed    reqStatus = "failed"
)

type client struct {
	clientID          string
	seqNum            int64
	currentWorkingInd int
	reqStatus         reqStatus
	reply             Reply
}

type job struct {
	ind  int
	op   Op
	done chan Op
}

func (kv *KVServer) updateAppliedInd(ind int64) {
	atomic.StoreInt64(&kv.appliedInd, ind)
}

func (kv *KVServer) getAppliedInd() int64 {
	return atomic.LoadInt64(&kv.appliedInd)
}

func (kv *KVServer) startRequest(args Args, reply Reply) (raftLogInd int, success bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	cID := args.GetClientID()
	seqNum := args.GetSeqNum()

	c, ok := kv.clientTable[cID]
	if ok {
		if c.seqNum > seqNum {
			// stale request
			// discard
			kv.printf("Stale req: %v", args)
			return -1, false
		}

		if c.seqNum == seqNum {
			switch c.reqStatus {
			case successed:
				// successs request
				reply.SetErr(OK)
				reply.SetTime(time.Now().UnixNano())
				reply.SetValue(c.reply.GetValue())
				kv.printf("Handled req: %v, reply", args, c.reply)
				return -1, false
			case working:
				// become new handler
				return c.currentWorkingInd, true

			case failed:
				// submit another request

			}
		}
	}

	ind, _, ok := kv.rf.Start(Op{
		Type:     args.GetOp(),
		Key:      args.GetKey(),
		Value:    args.GetValue(),
		ClientID: cID,
		SeqNum:   seqNum,
	})

	if !ok {
		reply.SetErr(ErrWrongLeader)
		reply.SetTime(time.Now().UnixNano())
		kv.printf("I am no leader: %v", args)
		return -1, false
	}

	kv.clientTable[cID] = &client{
		clientID:          cID,
		seqNum:            seqNum,
		currentWorkingInd: ind,
		reqStatus:         working,
		reply:             nil,
	}
	return ind, true
}

func (kv *KVServer) setClientReqStatue(cID string, status reqStatus) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.clientTable[cID].reqStatus = status
}

func (kv *KVServer) setClientReply(cID string, reply Reply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.clientTable[cID].reply = reply
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.printf("get args: %v", args)
	opType := args.GetOp()
	cID := args.GetClientID()
	seqNum := args.GetSeqNum()

	reply.ClientID = cID
	reply.SeqNum = seqNum

	ind, ok := kv.startRequest(args, reply)
	if !ok {
		// request is handled or stale
		return
	}

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
		if opDone.Type != cleanUpOp {
			kv.setClientReqStatue(cID, failed)
		}
	} else {
		if opDone.Value != nil {
			reply.Err = OK
			v, _ := opDone.Value.(string)
			reply.Value = v
		} else {
			reply.Err = ErrNoKey
			reply.Value = ""
		}

		kv.setClientReply(cID, reply)
		kv.setClientReqStatue(cID, successed)
		kv.printf("%v:%v finished", cID, seqNum)
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.printf("put append args: %v", args)
	opType := args.GetOp()
	cID := args.GetClientID()
	seqNum := args.GetSeqNum()

	reply.ClientID = cID
	reply.SeqNum = seqNum

	ind, ok := kv.startRequest(args, reply)
	if !ok {
		// request is handled or stale
		return
	}

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
		if opDone.Type != cleanUpOp {
			kv.setClientReqStatue(cID, failed)
		}
	} else {
		reply.Err = OK
		kv.printf("%v:%v finished", cID, seqNum)
		kv.setClientReply(cID, reply)
		kv.setClientReqStatue(cID, successed)
	}
}

func (kv *KVServer) apply(msg raft.ApplyMsg) {
	if !msg.CommandValid {
		return
	}

	cmd, ok := msg.Command.(Op)
	if !ok {
		log.Printf("Invalid cmd: %v, discard\n", msg.Command)
		return
	}

	cmdInd := fmt.Sprintf("%v", msg.CommandIndex)
	entry, ok := kv.jobTable.Get(cmdInd)
	if !ok {
		kv.printf("no need to apply: %v", msg)
		return
	}

	jobs, _ := entry.([]job)
	kv.printf("JOBS: %v", jobs)
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

	kv.updateAppliedInd(int64(msg.CommandIndex))
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
			kv.printf("PPP %v", v)
			return s
		}

		kv.printf("FFF: %v", val+s)
		return val + s
	}
}

func (kv *KVServer) startLoop() {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for !kv.killed() {
			time.Sleep(100 * time.Millisecond)
		}

		cancel()
	}()

	// cleanup stale job
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				appliedInd := kv.getAppliedInd()
				failedOp := Op{
					Type:     cleanUpOp,
					ClientID: "",
					SeqNum:   -1,
				}
				for ind := int64(1); ind < appliedInd; ind++ {
					cmdInd := fmt.Sprintf("%v", ind)
					entry, ok := kv.jobTable.Get(cmdInd)
					if !ok {
						continue
					}

					jobs, _ := entry.([]job)
					for _, job := range jobs {
						job.done <- failedOp
						close(job.done)
					}

					kv.jobTable.Delete(cmdInd)
				}
			}

			time.Sleep(100 * time.Millisecond)
		}
	}()

	// apply
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case applyMsg := <-kv.applyCh:
				kv.printf("applyMsg: %v", applyMsg)
				kv.apply(applyMsg)
			}
		}
	}()
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

	kv.clientTable = make(map[string]*client)

	// You may need initialization code here.
	kv.startLoop()

	return kv
}
