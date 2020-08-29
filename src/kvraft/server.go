package kvraft

import (
	"context"
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

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  opType
	Key   string
	Value string
	ID    string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store    KVStore
	jobTable KVStore
}

type job struct {
	ID   string
	done chan interface{}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.ID = args.ID
	_, isProcessing := kv.jobTable.Get(args.ID)
	if isProcessing {
		return
	}

	_, _, ok := kv.rf.Start(Op{
		Type: getType,
		Key:  args.Key,
		ID:   args.ID,
	})

	if !ok {
		reply.Err = ErrWrongLeader
		reply.Time = time.Now().UnixNano()
		reply.LeaderID = kv.rf.GetCurrentLeader()
		return
	}

	done := make(chan interface{})
	kv.jobTable.Put(args.ID, job{
		ID:   args.ID,
		done: done,
	})

	v := <-done
	value, ok := v.(string)
	reply.Time = time.Now().UnixNano()
	if !ok {
		log.Printf("Invalid value: %v, discard\n", value)
		reply.Err = ErrUnknown
		return
	}

	if len(value) > 0 {
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
	}

	reply.Value = value
	kv.jobTable.Delete(args.ID)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	reply.ID = args.ID
	_, isProcessing := kv.jobTable.Get(args.ID)
	if isProcessing {
		return
	}

	_, _, ok := kv.rf.Start(Op{
		Type:  args.Op,
		Key:   args.Key,
		Value: args.Value,
		ID:    args.ID,
	})

	if !ok {
		reply.Err = ErrWrongLeader
		reply.Time = time.Now().UnixNano()
		reply.LeaderID = kv.rf.GetCurrentLeader()
		return
	}

	done := make(chan interface{})
	kv.jobTable.Put(args.ID, job{
		ID:   args.ID,
		done: done,
	})

	<-done
	reply.Time = time.Now().UnixNano()
	reply.Err = OK
	kv.jobTable.Delete(args.ID)
}

func (kv *KVServer) apply(msg raft.ApplyMsg) {
	cmd, ok := msg.Command.(Op)
	if !ok {
		log.Printf("Invalid cmd: %v, discard\n", msg.Command)
	}

	entry, ok := kv.jobTable.Get(cmd.ID)
	if !ok {
		log.Printf("Job ID: %v is handled\n", cmd.ID)
		return
	}

	job, _ := entry.(job)
	switch cmd.Type {
	case getType:
		v, ok := kv.store.Get(cmd.Key)
		if !ok {
			job.done <- ""
		} else {
			job.done <- v
		}
		close(job.done)
	case putType, appendType:
		DPrintf("kk: %v : %v", cmd.Key, cmd.Value)
		kv.store.Put(cmd.Key, cmd.Value)
		job.done <- struct{}{}
		close(job.done)
	default:
		log.Printf("Invalid cmd type: %v, discard\n", cmd.Type)
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

	// apply
	for i := 0; i < workerNum; i++ {
		go func() {
			select {
			case <-ctx.Done():
				return
			case applyMsg := <-kv.applyCh:
				kv.apply(applyMsg)
			}
		}()
	}
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

	// You may need initialization code here.
	kv.startLoop()

	return kv
}
