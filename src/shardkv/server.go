package shardkv

// import "../shardmaster"
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
	"../shardmaster"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	workerNum = 10
	noSuchKey = "NoSuchKey"
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

func (kvs *KVStore) Map(f func(interface{}, interface{}) interface{}) {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()

	for k, v := range kvs.kvTable {
		kvs.kvTable[k] = f(k, v)
	}
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
	Value    []byte
	ClientID string
	SeqNum   int64
	Shard    int
	Err      Err
}

type shardQueue struct {
	sync.Mutex
	queue []raft.ApplyMsg
	shard int
}

func (sq *shardQueue) enqueue(msg raft.ApplyMsg) {
	sq.Lock()
	defer sq.Unlock()

	sq.queue = append(sq.queue, msg)
}

func (sq *shardQueue) dequeue(msg *raft.ApplyMsg) bool {
	sq.Lock()
	defer sq.Unlock()

	if len(sq.queue) == 0 {
		return false
	}

	*msg = sq.queue[0]
	sq.queue = sq.queue[1:]
	return true
}

type ShardKV struct {
	sm           *shardmaster.Clerk
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int   // snapshot if log grows this big
	dead         int32 // set by Kill()

	// for configuration
	shardArgsChan     chan ShardArgs
	config            shardmaster.Config
	prevConfig        shardmaster.Config
	shardsLock        [shardmaster.NShards]*sync.RWMutex
	shardsQueue       [shardmaster.NShards]shardQueue
	queueForNewShards sync.WaitGroup

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
	Err               Err
}

type job struct {
	ind  int
	op   Op
	done chan Op
}

func (kv *ShardKV) isInShard(key string) bool {
	if key == configKey {
		return true
	}

	kv.mu.RLock()
	defer kv.mu.RUnlock()

	shard := key2shard(key)
	return kv.config.Shards[shard] == kv.gid
}

func (kv *ShardKV) getConfigNum() int {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	return kv.config.Num
}

func (kv *ShardKV) getConfig() shardmaster.Config {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	tmp := kv.config
	c := shardmaster.Config{
		Groups: map[int][]string{},
	}

	c.Num = tmp.Num
	c.Shards = tmp.Shards
	for gid, members := range tmp.Groups {
		c.Groups[gid] = make([]string, len(members))
		copy(c.Groups[gid], members)
	}

	return c
}

func (kv *ShardKV) getPrevConfig() shardmaster.Config {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	tmp := kv.prevConfig
	c := shardmaster.Config{
		Groups: map[int][]string{},
	}

	c.Num = tmp.Num
	c.Shards = tmp.Shards
	for gid, members := range tmp.Groups {
		c.Groups[gid] = make([]string, len(members))
		copy(c.Groups[gid], members)
	}

	return c
}

func (kv *ShardKV) setNewConfig(c shardmaster.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.printf("Set new config: %v", c)
	kv.prevConfig = kv.config
	kv.config = c
}

func (kv *ShardKV) updateShards(c shardmaster.Config) {
	prevConfig := kv.getPrevConfig()
	var oldShards [shardmaster.NShards]bool
	for shard, gid := range prevConfig.Shards {
		if gid != kv.gid {
			continue
		}

		oldShards[shard] = true
	}

	// gid:shard
	gidToshardsShouldPull := map[int][]int{}
	for shard, gid := range c.Shards {
		if gid != kv.gid {
			continue
		}

		if !oldShards[shard] {
			kv.printf("Lock shard: %v", shard)
			kv.shardsLock[shard].Lock()
			prevOwner := prevConfig.Shards[shard]
			gidToshardsShouldPull[prevOwner] = append(gidToshardsShouldPull[prevOwner], shard)
		}
	}

	for gid, shardsToPull := range gidToshardsShouldPull {
		kv.shardArgsChan <- ShardArgs{
			Header: Header{
				SeqNum:   int64(c.Num),
				ClientID: fmt.Sprintf("%d:%d", kv.gid, kv.me),
			},
			GID:          gid,
			Servers:      kv.config.Groups[gid],
			Config:       c,
			ShardsToPull: shardsToPull,
		}
	}
}

func (kv *ShardKV) doPull(args ShardArgs) {
	kv.printf("do Pull: %v", args)
	for {
		for _, name := range args.Servers {
			reply := ShardReply{}
			c := kv.make_end(name)
			ok := kv.sendPull(c, &args, &reply)
			if !ok || reply.Err != OK {
				kv.printf("resend pull: ok(%v) %v", ok, args)
				continue
			}

			kv.PullShardsResponse(&args, &reply)
			return
		}
	}
}

func (kv *ShardKV) PullShards(args *ShardArgs, reply *ShardReply) {
	kv.printf("PullShards: %v", args)
	if args.Config.Num != kv.getConfig().Num {
		reply.Err = ErrFail
		kv.printf("config num not match: %v, config: %v", args.Config.Num, kv.getConfig())
		return
	}

	reply.ConfigNum = args.Config.Num
	shardsToPull := map[int]bool{}
	for _, shard := range args.ShardsToPull {
		shardsToPull[shard] = true
	}

	data := map[string]string{}
	kv.mu.RLock()
	for k, v := range kv.store {
		if shardsToPull[key2shard(k)] {
			data[k] = v
		}
	}
	kv.mu.RUnlock()
	reply.Data = data
	reply.Err = OK
}

func (kv *ShardKV) PullShardsResponse(args *ShardArgs, reply *ShardReply) {
	kv.printf("PullShardsResponse: %v %v", args, reply)
	if args.Config.Num != reply.ConfigNum || kv.getConfig().Num != reply.ConfigNum {
		kv.printf("stale config: %v, %v, %v", args, reply, kv.getConfig())
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	for k, v := range reply.Data {
		kv.store[k] = v
	}

	for _, shard := range args.ShardsToPull {
		kv.printf("Unlock Shard: %v", shard)
		kv.shardsLock[shard].Unlock()
	}

	kv.printf("queueForNewShards: %v", kv.queueForNewShards)
	kv.queueForNewShards.Done()
}

func (kv *ShardKV) sendPull(target *labrpc.ClientEnd, args *ShardArgs, reply *ShardReply) bool {
	doneChan := make(chan bool)
	go func() {
		kv.printf("send pull to %v", target)
		doneChan <- target.Call("ShardKV.PullShards", args, reply)
		close(doneChan)
	}()

	select {
	case <-time.After(reqTimeout):
		return false
	case ok := <-doneChan:
		return ok
	}
}

func (kv *ShardKV) updateAppliedInd(ind int64) {
	atomic.StoreInt64(&kv.appliedInd, ind)
}

func (kv *ShardKV) getAppliedInd() int64 {
	return atomic.LoadInt64(&kv.appliedInd)
}

func (kv *ShardKV) updateAppliedTerm(term int64) {
	atomic.StoreInt64(&kv.appliedTerm, term)
}

func (kv *ShardKV) getAppliedTerm() int64 {
	return atomic.LoadInt64(&kv.appliedTerm)
}

func (kv *ShardKV) setClient(c *client) {
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

func (kv *ShardKV) getClient(cID string) (client, bool) {
	kv.ctMu.RLock()
	defer kv.ctMu.RUnlock()

	c, ok := kv.clientTable[cID]
	if ok {
		return *c, ok
	}

	return client{}, false
}

func (kv *ShardKV) copyClientTable() map[string]client {
	kv.ctMu.RLock()
	defer kv.ctMu.RUnlock()

	copiedClientTable := map[string]client{}
	for k, v := range kv.clientTable {
		c := *v
		copiedClientTable[k] = c
	}

	return copiedClientTable
}

func (kv *ShardKV) installClientTable(ct map[string]client) {
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
	}

	kv.printf("installing client table: %v", kv.clientTable)
}

func (kv *ShardKV) startRequest(args Args, reply Reply) (raftLogInd int, success bool) {
	key := args.GetKey()
	if !kv.isInShard(key) {
		reply.SetErr(ErrWrongGroup)
		kv.printf("wrong group: %v : %v", args, reply)
		return -1, false
	}

	header := args.GetHeader()
	cID := header.ClientID
	seqNum := header.SeqNum

	c, ok := kv.getClient(cID)
	if ok {
		if c.SeqNum > seqNum {
			// stale request
			// discard
			kv.printf("Stale req: %v, client data in table: %v", args, c)
			return -1, false
		}

		if c.SeqNum == seqNum {
			// successs request
			// reply.SetTime(time.Now().UnixNano())

			e := c.Err
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
		Key:      key,
		Value:    []byte(args.GetValue()),
		ClientID: cID,
		SeqNum:   seqNum,
		Shard:    key2shard(key),
	})

	if !ok {
		reply.SetErr(ErrWrongLeader)
		// reply.SetTime(time.Now().UnixNano())
		kv.printf("I am no leader: %v : %v", args, reply)
		return -1, false
	}

	return ind, true
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.printf("get args: %v, reply: %v", args, reply)
	header := args.GetHeader()
	opType := args.GetOp()
	cID := header.ClientID
	seqNum := header.SeqNum

	reply.SetHeader(Header{
		ClientID: cID,
		SeqNum:   seqNum,
	})

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
			Shard:    key2shard(args.GetKey()),
		},
		done: done,
	}))

	opDone := <-done
	// reply.Time = time.Now().UnixNano()
	if opDone.Type != opType || opDone.ClientID != cID || opDone.SeqNum != seqNum {
		reply.SetErr(ErrFail)
		kv.printf("%v failed", args)
		return
	}

	e := opDone.Err
	v := opDone.Value
	if string(v) == noSuchKey {
		e = ErrNoKey
		v = []byte("")
	}

	reply.SetErr(e)
	reply.SetValue(string(v))

	kv.printf("%v:%v finished", cID, seqNum)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.printf("put append args: %v, reply: %v", args, reply)
	header := args.GetHeader()
	opType := args.GetOp()
	cID := header.ClientID
	seqNum := header.SeqNum

	reply.SetHeader(Header{
		ClientID: cID,
		SeqNum:   seqNum,
	})

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
			Shard:    key2shard(args.GetKey()),
		},
		done: done,
	}))

	opDone := <-done
	// reply.Time = time.Now().UnixNano()
	if opDone.Type != opType || opDone.ClientID != cID || opDone.SeqNum != seqNum {
		reply.SetErr(ErrFail)
		kv.printf("%v failed", args)
		return
	}

	reply.SetErr(opDone.Err)
	kv.printf("%v:%v finished", cID, seqNum)
}

func (kv *ShardKV) setConfigRequest(c shardmaster.Config) {
	prevConfigNum := kv.getConfigNum()
	if prevConfigNum >= c.Num {
		return
	}

	opType := configType
	cID := configClientTD
	seqNum := int64(prevConfigNum)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(c)
	data := w.Bytes()

	ind, ok := kv.startRequest(&ConfigArgs{
		Config: data,
		Header: Header{
			ClientID: cID,
			SeqNum:   seqNum,
		},
	}, &ConfigReply{})

	if !ok {
		// request is handled or stale
		return
	}

	kv.jobTable.AtomicOp(fmt.Sprintf("%v", ind), kv.appendJobWrapper(job{
		ind: ind,
		op: Op{
			Type:     opType,
			ClientID: cID,
			SeqNum:   seqNum,
		},
		done: nil,
	}))
}

func (kv *ShardKV) applyOp(msg raft.ApplyMsg) {
	kv.printf("about to applyOp: %v", msg)
	cmd, ok := msg.Command.(Op)
	if !ok {
		log.Printf("Invalid cmd: %v, discard\n", msg.Command)
		return
	}

	kv.shardsLock[cmd.Shard].Lock()
	defer kv.shardsLock[cmd.Shard].Unlock()

	kv.printf("applyOp: %v", msg)

	switch cmd.Type {
	case getType:
		kv.mu.RLock()
		if v, ok := kv.store[cmd.Key]; !ok {
			cmd.Value = []byte(noSuchKey)
		} else {
			cmd.Value = []byte(v)
		}
		kv.mu.RUnlock()
	case putType:
		kv.mu.Lock()
		kv.store[cmd.Key] = string(cmd.Value)
		kv.mu.Unlock()
	case appendType:
		kv.mu.Lock()
		kv.store[cmd.Key] += string(cmd.Value)
		kv.mu.Unlock()
	}

	kv.finishJob(msg, cmd, OK)
	kv.printf("queueForNewShards: %v", kv.queueForNewShards)
	kv.queueForNewShards.Done()
}

func (kv *ShardKV) apply(msg raft.ApplyMsg) {
	cmd, ok := msg.Command.(Op)
	if !ok {
		log.Printf("Invalid cmd: %v, discard\n", msg.Command)
		return
	}

	isInShard := kv.isInShard(cmd.Key)
	c, ok := kv.getClient(cmd.ClientID)
	if isInShard && (!ok || c.SeqNum < cmd.SeqNum || (c.SeqNum == cmd.SeqNum && cmd.Type == getType)) {
		switch cmd.Type {
		case getType:
			if ok && c.SeqNum == cmd.SeqNum {
				cmd.Value = []byte(c.LastExecutedValue)
				break
			}

			kv.queueForNewShards.Add(1)
			kv.printf("queueForNewShards: %v", kv.queueForNewShards)
			kv.shardsQueue[cmd.Shard].enqueue(msg)
			return

		case putType, appendType:
			kv.queueForNewShards.Add(1)
			kv.printf("queueForNewShards: %v", kv.queueForNewShards)
			kv.shardsQueue[cmd.Shard].enqueue(msg)
			return

		case configType:
			r := bytes.NewBuffer(cmd.Value)
			d := labgob.NewDecoder(r)
			var c shardmaster.Config
			if d.Decode(&c) != nil {
				log.Fatalf("%d decode config error", kv.me)
			}

			kv.queueForNewShards.Wait()
			kv.queueForNewShards.Add(1)
			kv.printf("queueForNewShards: %v", kv.queueForNewShards)
			kv.setNewConfig(c)
			if c.Num <= 1 {
				kv.queueForNewShards.Done()
				kv.printf("queueForNewShards: %v", kv.queueForNewShards)
				break
			}

			kv.updateShards(c)

		default:
			log.Printf("Invalid cmd type: %v, discard\n", cmd.Type)
			return
		}
	}

	e := OK
	if !isInShard {
		e = ErrWrongGroup
	}

	kv.finishJob(msg, cmd, e)
}

func (kv *ShardKV) finishJob(msg raft.ApplyMsg, cmd Op, e Err) {
	kv.printf("finish job: %v, %v", msg, cmd)
	if cmd.Type == configType {
		r := bytes.NewBuffer(cmd.Value)
		d := labgob.NewDecoder(r)
		var config shardmaster.Config
		if d.Decode(&config) != nil {
			log.Fatalf("%d decode config error", kv.me)
		}

		cmd.Value = []byte(fmt.Sprintf("config{Num: %v, Shards: %v, Groups: %v}", config.Num, config.Shards, config.Groups))
	}

	kv.setClient(&client{
		ClientID:          cmd.ClientID,
		SeqNum:            cmd.SeqNum,
		AppliedInd:        msg.CommandIndex,
		AppliedTerm:       msg.CommandTerm,
		LastExecutedValue: string(cmd.Value),
		Err:               e,
	})

	cmdInd := fmt.Sprintf("%v", msg.CommandIndex)
	entry, ok := kv.jobTable.Get(cmdInd)
	if !ok {
		return
	}

	jobs, _ := entry.([]job)
	kv.printf("JOBS: %v", jobs)

	cmd.Err = e
	for _, job := range jobs {
		if job.done == nil {
			continue
		}

		job.done <- cmd
		close(job.done)
	}

	kv.jobTable.Delete(cmdInd)
}

func (kv *ShardKV) appendJobWrapper(j job) func(interface{}) interface{} {
	return func(v interface{}) interface{} {
		val, ok := v.([]job)
		if !ok {
			return []job{j}
		}

		return append(val, j)
	}
}

func (kv *ShardKV) appendWrapper(s string) func(interface{}) interface{} {
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

func (kv *ShardKV) cleanUpSatleReq() {
	// appliedInd := kv.getAppliedInd()
	/* for ind := int64(1); ind < appliedInd; ind++ {
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
	} */
}

func (kv *ShardKV) snapshot(indexInLog, cmdInd, term int) {
	if kv.maxraftstate == -1 || kv.maxraftstate > kv.rf.GetPersistentSize() {
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
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

func (kv *ShardKV) restoreStateFromSnapshot(msg raft.ApplyMsg) {
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
}

func (kv *ShardKV) processApplyMsg(msg raft.ApplyMsg) {
	switch msg.Type {
	case raft.StateMachineCmdEntry:
		appliedInd := kv.getAppliedInd()
		if appliedInd > int64(msg.CommandIndex) {
			return
		}

		kv.apply(msg)
		kv.snapshot(msg.IndexInLog, msg.CommandIndex, msg.CommandTerm)

	case raft.SnapshotEntry:
		kv.restoreStateFromSnapshot(msg)

	case raft.TermEntry:
		break
	}

	if int64(msg.CommandIndex) > kv.getAppliedInd() {
		kv.updateAppliedInd(int64(msg.CommandIndex))
	}

	if int64(msg.CommandTerm) > kv.getAppliedTerm() {
		kv.updateAppliedTerm(int64(msg.CommandTerm))
	}
}

func (kv *ShardKV) startLoop() {
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

	// get latest config
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				c := kv.sm.Query(-1)
				kv.setConfigRequest(c)
			}
		}

		time.Sleep(100 * time.Millisecond)
	}()

	// Pull Shards
	for i := 0; i < shardmaster.NShards; i++ {
		go func(i int) {
			for {
				select {
				case <-ctx.Done():
					return
				case shardArgs := <-kv.shardArgsChan:
					kv.doPull(shardArgs)
				}
			}
		}(i)
	}

	// apply Op
	for i := 0; i < shardmaster.NShards; i++ {
		go func(i int) {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					msg := raft.ApplyMsg{}
					if ok := kv.shardsQueue[i].dequeue(&msg); ok {
						kv.printf("Shard %v dequeue %v", i, msg)
						kv.applyOp(msg)
						break
					}

					time.Sleep(50 * time.Millisecond)
				}
			}
		}(i)
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// for debug
//
func (kv *ShardKV) printf(format string, a ...interface{}) {
	a = append([]interface{}{fmt.Sprintf("%v:%v", kv.gid, kv.me)}, a...)
	DPrintf("%v: "+format, a...)
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	applyCh := make(chan raft.ApplyMsg)
	kv := &ShardKV{
		sm:           shardmaster.MakeClerk(masters),
		me:           me,
		maxraftstate: maxraftstate,
		make_end:     make_end,
		gid:          gid,
		masters:      masters,
		applyCh:      applyCh,
		rf:           raft.Make(servers, me, persister, applyCh),

		shardArgsChan: make(chan ShardArgs),
		config: shardmaster.Config{
			Groups: map[int][]string{},
		},
		prevConfig: shardmaster.Config{
			Groups: map[int][]string{},
		},

		jobTable: KVStore{
			kvTable: make(map[string]interface{}),
		},
		store:       make(map[string]string),
		clientTable: make(map[string]*client),
	}
	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)
	for i := 0; i < shardmaster.NShards; i++ {
		kv.shardsLock[i] = &sync.RWMutex{}
		kv.shardsQueue[i] = shardQueue{
			queue: []raft.ApplyMsg{},
			shard: i,
		}
	}

	kv.startLoop()

	return kv
}
