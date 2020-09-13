package kvraft

import (
	"bytes"
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
	noSuchKey   = "NoSuchKey"
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

func (kvs *KVStore) Copy() map[string]interface{} {
	kvs.mu.RLock()
	defer kvs.mu.RUnlock()

	copiedMap := map[string]interface{}{}
	for k, v := range kvs.kvTable {
		copiedMap[k] = v
	}

	return copiedMap
}

func (kvs *KVStore) Replace(newData map[string]interface{}) {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()

	kvs.kvTable = newData
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type     opType
	Key      string
	Value    string
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
	store    map[string]string
	jobTable KVStore

	clientTable map[string]*client
	ctMu        sync.RWMutex

	appliedInd  int64
	appliedTerm int64
}

type client struct {
	ClientID          string
	SeqNum            int64
	AppliedInd        int
	AppliedTerm       int
	LastExecutedValue string
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

func (kv *KVServer) updateAppliedTerm(term int64) {
	atomic.StoreInt64(&kv.appliedTerm, term)
}

func (kv *KVServer) getAppliedTerm() int64 {
	return atomic.LoadInt64(&kv.appliedTerm)
}

func (kv *KVServer) setClient(c *client) {
	kv.ctMu.Lock()
	defer kv.ctMu.Unlock()

	client, ok := kv.clientTable[c.ClientID]
	if !ok {
		kv.clientTable[c.ClientID] = c
		return
	}

	kv.printf("set new client: %v", c)
	client.SeqNum = c.SeqNum
	client.AppliedInd = c.AppliedInd
	client.AppliedTerm = c.AppliedTerm
	client.LastExecutedValue = c.LastExecutedValue
}

func (kv *KVServer) getClient(cID string) (client, bool) {
	kv.ctMu.RLock()
	defer kv.ctMu.RUnlock()

	c, ok := kv.clientTable[cID]
	if ok {
		return *c, ok
	}

	return client{}, false
}

func (kv *KVServer) copyClientTable() map[string]client {
	kv.ctMu.RLock()
	defer kv.ctMu.RUnlock()

	copiedClientTable := map[string]client{}
	for k, v := range kv.clientTable {
		c := *v
		copiedClientTable[k] = c
		kv.printf("copying client table: %v", copiedClientTable)
	}

	return copiedClientTable
}

func (kv *KVServer) installClientTable(ct map[string]client) {
	kv.ctMu.Lock()
	defer kv.ctMu.Unlock()

	for k, c := range ct {
		kv.clientTable[k] = &client{
			ClientID:          c.ClientID,
			SeqNum:            c.SeqNum,
			AppliedInd:        c.AppliedInd,
			AppliedTerm:       c.AppliedTerm,
			LastExecutedValue: c.LastExecutedValue,
		}
		kv.printf("installing client table: %v", kv.clientTable)
	}
}

func (kv *KVServer) startRequest(args Args, reply Reply) (raftLogInd int, success bool) {
	cID := args.GetClientID()
	seqNum := args.GetSeqNum()

	c, ok := kv.getClient(cID)
	if ok {
		if c.SeqNum > seqNum {
			// stale request
			// discard
			kv.printf("Stale req: %v, client datta in table: %v", args, c)
			return -1, false
		}

		if c.SeqNum == seqNum {
			// successs request
			// reply.SetTime(time.Now().UnixNano())

			e := OK
			v := c.LastExecutedValue
			if v == noSuchKey {
				e = ErrNoKey
				v = ""
			}

			reply.SetErr(e)
			reply.SetValue(v)

			kv.printf("Handled req: %v, reply", args, c.LastExecutedValue)
			return -1, false
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
		// reply.SetTime(time.Now().UnixNano())
		kv.printf("I am no leader: %v : %v", args, reply)
		return -1, false
	}

	return ind, true
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.printf("get args: %v, reply: %v", args, reply)
	opType := args.GetOp()
	cID := args.GetClientID()
	seqNum := args.GetSeqNum()

	reply.SetClientID(cID)
	reply.SetSeqNum(seqNum)

	ind, ok := kv.startRequest(args, reply)
	if !ok {
		// request is handled or stale
		kv.printf("Dont handle %v : %v", args, reply)
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
	// reply.Time = time.Now().UnixNano()
	if opDone.Type != opType || opDone.ClientID != cID || opDone.SeqNum != seqNum {
		reply.SetErr(ErrFail)
		kv.printf("%v:%v failed", cID, seqNum)
		return
	}

	e := OK
	v := opDone.Value
	if v == noSuchKey {
		e = ErrNoKey
		v = ""
	}

	reply.SetErr(e)
	reply.SetValue(v)

	kv.printf("%v:%v finished", cID, seqNum)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.printf("put append args: %v, reply: %v", args, reply)
	opType := args.GetOp()
	cID := args.GetClientID()
	seqNum := args.GetSeqNum()

	reply.SetClientID(cID)
	reply.SetSeqNum(seqNum)

	ind, ok := kv.startRequest(args, reply)
	if !ok {
		// request is handled or stale
		kv.printf("Dont handle %v : %v", args, reply)
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
	// reply.Time = time.Now().UnixNano()
	if opDone.Type != opType || opDone.ClientID != cID || opDone.SeqNum != seqNum {
		reply.SetErr(ErrFail)
		kv.printf("%v:%v failed", cID, seqNum)
		return
	}

	reply.SetErr(OK)
	kv.printf("%v:%v finished", cID, seqNum)
}

func (kv *KVServer) apply(msg raft.ApplyMsg) {
	cmd, ok := msg.Command.(Op)
	if !ok {
		log.Printf("Invalid cmd: %v, discard\n", msg.Command)
		return
	}

	c, ok := kv.getClient(cmd.ClientID)
	if !ok || c.SeqNum < cmd.SeqNum || (c.SeqNum == cmd.SeqNum && cmd.Type == getType) {
		switch cmd.Type {
		case getType:
			if ok && c.SeqNum == cmd.SeqNum {
				cmd.Value = c.LastExecutedValue
				break
			}

			if v, ok := kv.store[cmd.Key]; !ok {
				cmd.Value = noSuchKey
			} else {
				cmd.Value = v
			}
		case putType:
			kv.store[cmd.Key] = cmd.Value
		case appendType:
			kv.store[cmd.Key] += cmd.Value
		default:
			log.Printf("Invalid cmd type: %v, discard\n", cmd.Type)
			return
		}
	}

	kv.setClient(&client{
		ClientID:          cmd.ClientID,
		SeqNum:            cmd.SeqNum,
		AppliedInd:        msg.CommandIndex,
		AppliedTerm:       msg.CommandTerm,
		LastExecutedValue: cmd.Value,
	})

	cmdInd := fmt.Sprintf("%v", msg.CommandIndex)
	entry, ok := kv.jobTable.Get(cmdInd)
	if !ok {
		return
	}

	jobs, _ := entry.([]job)
	kv.printf("JOBS: %v", jobs)

	for _, job := range jobs {
		job.done <- cmd
		close(job.done)
	}

	kv.jobTable.Delete(cmdInd)
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

func (kv *KVServer) cleanUpSatleReq() {
	appliedInd := kv.getAppliedInd()
	for ind := int64(1); ind < appliedInd; ind++ {
		cmdInd := fmt.Sprintf("%v", ind)
		entry, ok := kv.jobTable.Get(cmdInd)
		if !ok {
			continue
		}

		jobs, _ := entry.([]job)
		for _, job := range jobs {
			close(job.done)
		}

		kv.jobTable.Delete(cmdInd)
	}
}

func (kv *KVServer) snapshot(indexInLog, cmdInd, term int) {
	if kv.maxraftstate == -1 || kv.maxraftstate > kv.rf.GetPersistentSize() {
		return
	}

	clientTable := kv.copyClientTable()
	kv.printf("client table to be snapshoted: %v", clientTable)
	kv.printf("store to be snapshoted: %v", kv.store)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.store)
	e.Encode(clientTable)
	data := w.Bytes()
	kv.rf.Snapshot(data, indexInLog, cmdInd, term)
}

func (kv *KVServer) restoreStateFromSnapshot(msg raft.ApplyMsg) {
	snapshot, ok := msg.Command.([]byte)
	if !ok {
		log.Fatalf("%v failed to restore from snapshot", kv.me)
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if snapshot == nil || len(snapshot) < 1 || int64(msg.CommandIndex) < kv.appliedInd {
		kv.printf("reject installsnapshot: %v", msg)
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var kvstore map[string]string
	var clientTable map[string]client

	if d.Decode(&kvstore) != nil || d.Decode(&clientTable) != nil {
		log.Fatalf("%d restore failed", kv.me)
	}

	kv.printf("instal lsnapshot go go: %v", msg)
	kv.printf("original store: %v", kv.store)
	kv.store = kvstore
	kv.printf("new store: %v", kvstore)

	kv.printf("original client table: %v", kv.clientTable)
	kv.printf("new client table: %v", clientTable)
	kv.installClientTable(clientTable)
	kv.updateAppliedInd(int64(msg.CommandIndex))
	kv.updateAppliedTerm(int64(msg.CommandTerm))
}

func (kv *KVServer) processApplyMsg(msg raft.ApplyMsg) {
	switch msg.Type {
	case raft.StateMachineCmdEntry:
		kv.apply(msg)
		kv.updateAppliedInd(int64(msg.CommandIndex))
		if int64(msg.CommandTerm) > kv.getAppliedTerm() {
			kv.updateAppliedTerm(int64(msg.CommandTerm))
		}
		kv.snapshot(msg.IndexInLog, msg.CommandIndex, msg.CommandTerm)
	case raft.SnapshotEntry:
		kv.restoreStateFromSnapshot(msg)
	case raft.TermEntry:
		break
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
				kv.cleanUpSatleReq()
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
				kv.processApplyMsg(applyMsg)
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

	applyCh := make(chan raft.ApplyMsg)
	kv := &KVServer{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      applyCh,
		rf:           raft.Make(servers, me, persister, applyCh),

		jobTable: KVStore{
			kvTable: make(map[string]interface{}),
		},
		store:       make(map[string]string),
		clientTable: make(map[string]*client),
	}

	// You may need initialization code here.
	kv.startLoop()

	return kv
}
