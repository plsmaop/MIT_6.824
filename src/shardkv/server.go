package shardkv

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

func (kvs *KVStore) ForEach(f func(interface{}, interface{})) {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()

	for k, v := range kvs.kvTable {
		f(k, v)
	}
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
	Value    interface{}
	ClientID string
	SeqNum   int64
	Shard    int
	Err      Err
}

type shardQueue struct {
	sync.RWMutex
	queue []raft.ApplyMsg
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

func (sq *shardQueue) copyElem(q *[]raft.ApplyMsg) {
	sq.RLock()
	defer sq.RUnlock()

	copy(*q, sq.queue)
}

func (sq *shardQueue) len() int {
	sq.RLock()
	defer sq.RUnlock()

	return len(sq.queue)
}

func (sq *shardQueue) merge(q []raft.ApplyMsg) {
	sq.Lock()
	defer sq.Unlock()

	tmpQ := []raft.ApplyMsg{}
	i := 0
	j := 0
	for i < len(q) && j < len(sq.queue) {
		if q[i].CommandIndex == sq.queue[j].CommandIndex {
			tmpQ = append(tmpQ, sq.queue[j])
			i++
			j++
			continue
		}

		if q[i].CommandIndex > sq.queue[j].CommandIndex {
			tmpQ = append(tmpQ, sq.queue[j])
			j++
			continue
		}

		tmpQ = append(tmpQ, q[i])
		i++
	}

	for i < len(q) {
		tmpQ = append(tmpQ, q[i])
		i++
	}

	for j < len(sq.queue) {
		tmpQ = append(tmpQ, sq.queue[j])
		j++
	}

	sq.queue = tmpQ
}

func (sq *shardQueue) setQueue(q []raft.ApplyMsg) {
	sq.Lock()
	defer sq.Unlock()

	sq.queue = q
}

type ShardStoreData struct {
	Store       map[string]string
	Shard       int
	ClientTable map[string]client
}

type ShardStore struct {
	ShardStoreData
	mu    sync.RWMutex
	queue shardQueue
}

//
// must be used in critical section
//
func (ss *ShardStore) get(key string) (string, bool) {
	v, ok := ss.Store[key]
	if !ok {
		return "", false
	}

	return v, ok
}

//
// must be used in critical section
//
func (ss *ShardStore) put(key string, val string) {
	ss.Store[key] = val
}

//
// must be used in critical section
//
func (ss *ShardStore) append(key string, val string) {
	ss.Store[key] += val
}

//
// must be used in critical section
//
func (ss *ShardStore) setClient(c client) {
	client, ok := ss.ClientTable[c.ClientID]
	if !ok {
		ss.ClientTable[c.ClientID] = c
		return
	}

	if c.SeqNum <= client.SeqNum {
		return
	}

	ss.ClientTable[c.ClientID] = c
}

//
// must be used in critical section
//
func (ss *ShardStore) getClient(cID string) (client, bool) {
	c, ok := ss.ClientTable[cID]
	if ok {
		return c, ok
	}

	return client{}, false
}

//
// must be used in critical section
//
func (ss *ShardStore) getClientTable() map[string]client {
	ct := map[string]client{}
	for k, v := range ss.ClientTable {
		ct[k] = v
	}

	return ct
}

func (ss *ShardStore) getClientWithLock(cID string) (client, bool) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	return ss.getClient(cID)
}

func (ss *ShardStore) getClientTableWithLock() map[string]client {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	return ss.getClientTable()
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
	clientID          string
	shardArgsChan     chan ShardArgs
	config            shardmaster.Config
	prevConfig        shardmaster.Config
	queueForNewShards sync.WaitGroup

	// Your definitions here.
	shardStores [shardmaster.NShards]*ShardStore
	jobTable    KVStore

	configNumToStoreSnapshot map[int][shardmaster.NShards]ShardStoreData
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

func (kv *ShardKV) finishPull(shardsToPull []int) {
	for _, shard := range shardsToPull {
		kv.printf("Unlock Shard: %v", shard)
		kv.shardStores[shard].mu.Unlock()
	}

	kv.queueForNewShards.Done()
	// kv.printf("queueForNewShards Done(finishPull): %v", kv.queueForNewShards)
}

//
// must be used in critical section
//
func (kv *ShardKV) snapshotForConfigChange() {
	var storeSnapshot [shardmaster.NShards]ShardStoreData
	for shard := 0; shard < len(storeSnapshot); shard++ {
		if kv.config.Shards[shard] != kv.gid {
			continue
		}

		if storeSnapshot[shard].Store == nil {
			storeSnapshot[shard].Store = map[string]string{}
			storeSnapshot[shard].ClientTable = map[string]client{}
		}

		// kv.shardStores[shard].mu.RLock()

		for k, v := range kv.shardStores[shard].Store {
			storeSnapshot[shard].Store[k] = v
		}

		for k, v := range kv.shardStores[shard].ClientTable {
			storeSnapshot[shard].ClientTable[k] = v
		}

		// kv.shardStores[shard].mu.RUnlock()
	}

	kv.configNumToStoreSnapshot[kv.config.Num] = storeSnapshot
	kv.printf("configNumToStoreSnapshot: %v", kv.configNumToStoreSnapshot)
}

func (kv *ShardKV) updateShards(config shardmaster.Config) {
	kv.printf("about to update: %v", config)
	kv.mu.Lock()

	if kv.config.Num >= config.Num {
		kv.mu.Unlock()
		return
	}

	kv.snapshotForConfigChange()
	kv.prevConfig = kv.config
	kv.config = config
	if config.Num <= 1 {
		kv.mu.Unlock()
		return
	}

	var oldShards [shardmaster.NShards]bool
	for shard, gid := range kv.prevConfig.Shards {
		if gid != kv.gid {
			continue
		}

		oldShards[shard] = true
	}

	// gid:shard
	gidToShardsShouldPull := map[int][]int{}
	for shard, gid := range config.Shards {
		if gid != kv.gid {
			continue
		}

		if !oldShards[shard] {
			kv.printf("Lock shard: %v", shard)
			kv.shardStores[shard].mu.Lock()
			prevOwner := kv.prevConfig.Shards[shard]
			gidToShardsShouldPull[prevOwner] = append(gidToShardsShouldPull[prevOwner], shard)
		}
	}

	kv.printf("gidToShardsShouldPull: %v", gidToShardsShouldPull)
	if len(gidToShardsShouldPull) == 0 {
		kv.mu.Unlock()
		return
	}

	kv.queueForNewShards.Add(len(gidToShardsShouldPull))
	// kv.printf("queueForNewShards Add(config pull): %v", kv.queueForNewShards)
	kv.mu.Unlock()

	for gid, shardsToPull := range gidToShardsShouldPull {
		servers := kv.prevConfig.Groups[gid]
		if len(servers) == 0 {
			kv.finishPull(shardsToPull)

			continue
		}

		kv.printf("try pull %v from (%v)%v", shardsToPull, gid, servers)
		kv.shardArgsChan <- ShardArgs{
			Header: Header{
				SeqNum:   int64(config.Num),
				ClientID: fmt.Sprintf("%d:%d", kv.gid, kv.me),
			},
			GID:          gid,
			Servers:      servers,
			Config:       config,
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
				time.Sleep(pullShardWaitTime)
				continue
			}

			kv.PullShardsResponse(&args, &reply)
			return
		}
	}
}

func (kv *ShardKV) PullShards(args *ShardArgs, reply *ShardReply) {
	kv.printf("PullShards: %v", args)

	kv.mu.RLock()
	defer kv.mu.RUnlock()

	c := kv.config
	if args.Config.Num > c.Num {
		reply.Err = ErrFail
		kv.printf("config num not match: %v, config: %v", args.Config.Num, kv.config)
		return
	}

	reply.Err = OK
	reply.Config = args.Config
	reply.Header = Header{
		SeqNum:   int64(args.Config.Num),
		ClientID: fmt.Sprintf("%d:%d", kv.gid, kv.me),
	}

	kv.printf("PULL kv.configNumToStoreSnapshot: %v", kv.configNumToStoreSnapshot)
	kv.printf("prev config num: %v", args.Config.Num-1)
	storeSnapshot := kv.configNumToStoreSnapshot[args.Config.Num-1]

	for _, shard := range args.ShardsToPull {
		if reply.Data[shard].Store == nil {
			reply.Data[shard].Store = map[string]string{}
			reply.Data[shard].ClientTable = map[string]client{}
		}

		for k, v := range storeSnapshot[shard].Store {
			reply.Data[shard].Store[k] = v
		}

		for k, v := range storeSnapshot[shard].ClientTable {
			reply.Data[shard].ClientTable[k] = v
		}
	}
}

func (kv *ShardKV) PullShardsResponse(args *ShardArgs, reply *ShardReply) {
	kv.printf("PullShardsResponse: %v %v", args, reply)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.Config.Num != reply.Config.Num {
		kv.printf("stale config: %v, %v, %v", args, reply, kv.config)
		return
	}

	for _, shard := range args.ShardsToPull {
		kv.shardStores[shard].Store = reply.Data[shard].Store
		kv.shardStores[shard].ClientTable = reply.Data[shard].ClientTable
	}

	kv.finishPull(args.ShardsToPull)
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
	shard := key2shard(key)

	c, ok := kv.shardStores[shard].getClientWithLock(cID)
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

			reply.SetErr(OK)
			reply.SetValue(c.LastExecutedValue)

			kv.printf("Handled req: %v, reply", args, c.LastExecutedValue)
			return -1, false
		}
	}

	ind, _, ok := kv.rf.Start(Op{
		Type:     args.GetOp(),
		Key:      key,
		Value:    args.GetValue(),
		ClientID: cID,
		SeqNum:   seqNum,
		Shard:    shard,
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

	kv.printf("opDone: %v", opDone)
	v, _ := opDone.Value.(string)
	reply.SetErr(opDone.Err)
	reply.SetValue(v)

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
	// kv.printf("prevConfigNum: %v, config: %v", prevConfigNum, c)
	if prevConfigNum >= c.Num {
		return
	}

	opType := configType
	cID := configClientTD
	seqNum := int64(c.Num)
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

//
// must be used in critical seciont
//
func (kv *ShardKV) applyOp(msg raft.ApplyMsg) {
	defer kv.queueForNewShards.Done()

	cmd, ok := msg.Command.(Op)
	if !ok {
		log.Printf("Invalid cmd: %v, discard\n", msg.Command)
		return
	}

	shardStore := kv.shardStores[cmd.Shard]
	kv.printf("applyOp: %v", msg)

	e := OK
	c, ok := shardStore.getClient(cmd.ClientID)
	if !ok || c.SeqNum < cmd.SeqNum || (c.SeqNum == cmd.SeqNum && cmd.Type == getType) {
		switch cmd.Type {
		case getType:
			if ok && c.SeqNum == cmd.SeqNum {
				cmd.Value = c.LastExecutedValue
				break
			}

			if v, ok := shardStore.get(cmd.Key); !ok {
				cmd.Value = ""
				e = ErrNoKey
			} else {
				cmd.Value = v
			}
		case putType:
			s, _ := cmd.Value.(string)
			shardStore.put(cmd.Key, s)
		case appendType:
			s, _ := cmd.Value.(string)
			shardStore.append(cmd.Key, s)
		}
	}

	kv.finishJob(msg, cmd, e)
	// kv.printf("queueForNewShards Done(applyOp): %v", kv.queueForNewShards)
}

func (kv *ShardKV) processOp(shard int) bool {
	kv.shardStores[shard].mu.Lock()
	defer kv.shardStores[shard].mu.Unlock()

	msg := raft.ApplyMsg{}
	if ok := kv.shardStores[shard].queue.dequeue(&msg); ok {
		kv.printf("Shard %v dequeue %v", shard, msg)
		kv.applyOp(msg)
		return true
	}

	return false
}

func (kv *ShardKV) applyConfig(cmd Op) {
	b, _ := cmd.Value.([]byte)
	r := bytes.NewBuffer(b)
	d := labgob.NewDecoder(r)
	var c shardmaster.Config
	if d.Decode(&c) != nil {
		log.Fatalf("%d decode config error", kv.me)
	}

	// kv.printf("queueForNewShards Add(wait): %v", kv.queueForNewShards)
	kv.queueForNewShards.Wait()

	kv.updateShards(c)
}

func (kv *ShardKV) apply(msg raft.ApplyMsg) {
	cmd, ok := msg.Command.(Op)
	if !ok {
		log.Printf("Invalid cmd: %v, discard\n", msg.Command)
		return
	}

	if cmd.Type == configType {
		kv.applyConfig(cmd)
		kv.finishJob(msg, cmd, OK)
		return
	}

	if !kv.isInShard(cmd.Key) {
		return
	}

	shard := cmd.Shard
	shardStore := kv.shardStores[shard]
	shardStore.mu.Lock()
	defer shardStore.mu.Unlock()

	c, ok := shardStore.getClient(cmd.ClientID)
	if !ok || c.SeqNum < cmd.SeqNum || (c.SeqNum == cmd.SeqNum && cmd.Type == getType) {
		switch cmd.Type {
		case getType:
			if ok && c.SeqNum == cmd.SeqNum {
				cmd.Value = c.LastExecutedValue
				break
			}

			kv.queueForNewShards.Add(1)
			// kv.printf("queueForNewShards Add(get): %v", kv.queueForNewShards)
			kv.shardStores[cmd.Shard].queue.enqueue(msg)
			return

		case putType, appendType:
			kv.queueForNewShards.Add(1)
			// kv.printf("queueForNewShards Add(put,append): %v", kv.queueForNewShards)
			kv.shardStores[cmd.Shard].queue.enqueue(msg)
			return

		default:
			log.Printf("Invalid cmd type: %v, discard\n", cmd.Type)
			return
		}
	}

	kv.finishJob(msg, cmd, OK)
}

//
// must be used in critical section
//
func (kv *ShardKV) finishJob(msg raft.ApplyMsg, cmd Op, e Err) {
	kv.printf("finish job: %v, %v", msg, cmd)
	if cmd.Type != configType {
		v, _ := cmd.Value.(string)

		kv.shardStores[cmd.Shard].setClient(client{
			ClientID:          cmd.ClientID,
			SeqNum:            cmd.SeqNum,
			AppliedInd:        msg.CommandIndex,
			AppliedTerm:       msg.CommandTerm,
			LastExecutedValue: v,
		})
	}

	cmdInd := fmt.Sprintf("%v", msg.CommandIndex)
	kv.printf("msg.CommandIndex: %v done", msg.CommandIndex)
	entry, ok := kv.jobTable.Get(cmdInd)
	if !ok {
		kv.printf("No need to handle jobTable, cmd: %v", cmd)
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
	jobsToDelete := []job{}

	var cts [shardmaster.NShards]map[string]client
	for shard, shardStore := range kv.shardStores {
		cts[shard] = shardStore.getClientTableWithLock()
	}

	kv.jobTable.ForEach(func(_, v interface{}) {
		job, _ := v.(job)
		for shard := range kv.shardStores {
			client, ok := cts[shard][job.op.ClientID]
			if !ok {
				continue
			}

			if job.op.SeqNum < client.SeqNum {
				jobsToDelete = append(jobsToDelete, job)
			}
		}
	})

	for _, jobToDel := range jobsToDelete {
		cmdInd := fmt.Sprintf("%v", jobToDel.ind)
		entry, ok := kv.jobTable.Get(cmdInd)
		if !ok {
			continue
		}

		jobs, _ := entry.([]job)
		for _, job := range jobs {
			if job.op.ClientID == jobToDel.op.ClientID && job.op.SeqNum == jobToDel.op.SeqNum {
				if job.done != nil {
					close(job.done)
				}
			}
		}

		kv.jobTable.Delete(cmdInd)
	}
}

func (kv *ShardKV) snapshot(indexInLog, cmdInd, term int) {
	if kv.maxraftstate == -1 || kv.maxraftstate > kv.rf.GetPersistentSize() {
		return
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	config := kv.getConfig()
	prevConfig := kv.getPrevConfig()
	kv.printf("config to be snapshtoed: %v", config)

	snapshotData := SnapshotData{}
	for shard, shardStore := range kv.shardStores {
		if config.Shards[shard] != kv.gid {
			continue
		}

		shardStore.mu.RLock()

		store := map[string]string{}
		for k, v := range shardStore.Store {
			store[k] = v
		}

		kv.printf("store to be snapshoted: %v", store)
		snapshotData.Stores[shard] = store

		cmdToExec := make([]raft.ApplyMsg, shardStore.queue.len())
		shardStore.queue.copyElem(&cmdToExec)
		snapshotData.CmdToExec[shard] = cmdToExec
		kv.printf("cmdToExec: %v", cmdToExec)

		kv.printf("client table to be snapshoted: %v", shardStore.ClientTable)

		clientTable := map[string]client{}
		for cID, client := range shardStore.ClientTable {
			clientTable[cID] = client
		}
		snapshotData.ClientTables[shard] = clientTable

		shardStore.mu.RUnlock()
	}

	kv.printf("snapshotData: %v", snapshotData)

	e.Encode(snapshotData)
	e.Encode(kv.configNumToStoreSnapshot)
	e.Encode(prevConfig)
	e.Encode(config)
	data := w.Bytes()
	kv.rf.Snapshot(data, indexInLog, cmdInd, term)
	kv.printf("finish snapshot")
}

func (kv *ShardKV) restoreStateFromSnapshot(msg raft.ApplyMsg) {
	snapshot, ok := msg.Command.([]byte)
	if !ok {
		log.Fatalf("%v failed to restore from snapshot", kv.me)
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var snapshotData SnapshotData
	var configNumToStoreSnapshot map[int][shardmaster.NShards]ShardStoreData
	var prevConfig, config shardmaster.Config

	if d.Decode(&snapshotData) != nil || d.Decode(&configNumToStoreSnapshot) != nil || d.Decode(&prevConfig) != nil || d.Decode(&config) != nil {
		log.Fatalf("%d restore failed", kv.me)
	}

	kv.printf("install snapshot go go: %v", msg)
	kv.printf("original store: %v", kv.shardStores)

	for shard, oldShardStore := range kv.shardStores {
		if config.Shards[shard] != kv.gid {
			continue
		}

		oldShardStore.mu.Lock()
		kv.printf("snapshotData.CmdToExec[%v]: %v", shard, snapshotData.CmdToExec[shard])

		// add for new cmd to be executed
		kv.queueForNewShards.Add(len(snapshotData.CmdToExec[shard]))

		// minus stale cmd
		kv.queueForNewShards.Add(-kv.shardStores[shard].queue.len())

		// set new queue for cmd to be executed
		kv.shardStores[shard].queue.setQueue(snapshotData.CmdToExec[shard])

		kv.printf("New Queue(shard: %v): %v", shard, kv.shardStores[shard].queue)
		kv.printf("original client table: %v", oldShardStore.ClientTable)

		oldShardStore.Store = snapshotData.Stores[shard]
		oldShardStore.ClientTable = snapshotData.ClientTables[shard]

		kv.printf("new client table: %v", oldShardStore.ClientTable)

		oldShardStore.mu.Unlock()
	}

	kv.printf("new store: %v", snapshotData)
	kv.printf("new configNumToStoreSnapshot: %v", configNumToStoreSnapshot)
	kv.configNumToStoreSnapshot = configNumToStoreSnapshot

	// update config
	kv.mu.Lock()
	kv.prevConfig = prevConfig
	kv.config = config
	kv.mu.Unlock()
	kv.printf("finish srestore napshot")
}

func (kv *ShardKV) processApplyMsg(msg raft.ApplyMsg) {
	switch msg.Type {
	case raft.StateMachineCmdEntry:
		kv.apply(msg)
		kv.snapshot(msg.IndexInLog, msg.CommandIndex, msg.CommandTerm)

	case raft.SnapshotEntry:
		kv.restoreStateFromSnapshot(msg)

	case raft.TermEntry:
		break
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
				c := kv.sm.Query(kv.getConfigNum() + 1)
				kv.setConfigRequest(c)
			}

			time.Sleep(100 * time.Millisecond)
		}
	}()

	// Apply Op
	for i := 0; i < shardmaster.NShards; i++ {
		go func(i int) {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					if kv.processOp(i) {
						break
					}

					time.Sleep(50 * time.Millisecond)
				}
			}
		}(i)
	}

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

		clientID:      fmt.Sprintf("%d:%d", gid, me),
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

		configNumToStoreSnapshot: map[int][shardmaster.NShards]ShardStoreData{},
	}
	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)
	for i := 0; i < shardmaster.NShards; i++ {
		kv.shardStores[i] = &ShardStore{
			ShardStoreData: ShardStoreData{
				Store:       map[string]string{},
				Shard:       i,
				ClientTable: map[string]client{},
			},
			queue: shardQueue{
				queue: []raft.ApplyMsg{},
			},
		}
	}

	kv.startLoop()

	return kv
}
