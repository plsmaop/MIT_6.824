package shardmaster

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
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

type ShardMaster struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	maxraftstate int

	// Your data here.
	jobTable    KVStore
	clientTable map[string]*client
	ctMu        sync.RWMutex
	appliedInd  int64
	appliedTerm int64

	configs []Config // indexed by config num
}

type client struct {
	ClientID          string
	SeqNum            int64
	AppliedInd        int
	AppliedTerm       int
	LastExecutedValue interface{}
}

type job struct {
	ind  int
	op   Op
	done chan Op
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type     opType
	Value    interface{}
	ClientID string
	SeqNum   int64
}

func (sm *ShardMaster) updateAppliedInd(ind int64) {
	atomic.StoreInt64(&sm.appliedInd, ind)
}

func (sm *ShardMaster) getAppliedInd() int64 {
	return atomic.LoadInt64(&sm.appliedInd)
}

func (sm *ShardMaster) updateAppliedTerm(term int64) {
	atomic.StoreInt64(&sm.appliedTerm, term)
}

func (sm *ShardMaster) getAppliedTerm() int64 {
	return atomic.LoadInt64(&sm.appliedTerm)
}

func (sm *ShardMaster) setClient(c *client) {
	sm.ctMu.Lock()
	defer sm.ctMu.Unlock()

	client, ok := sm.clientTable[c.ClientID]
	if !ok {
		sm.clientTable[c.ClientID] = c
		return
	}

	sm.printf("set new client: %v", c)
	client.SeqNum = c.SeqNum
	client.AppliedInd = c.AppliedInd
	client.AppliedTerm = c.AppliedTerm
	client.LastExecutedValue = c.LastExecutedValue
}

func (sm *ShardMaster) getClient(cID string) (client, bool) {
	sm.ctMu.RLock()
	defer sm.ctMu.RUnlock()

	c, ok := sm.clientTable[cID]
	if ok {
		return *c, ok
	}

	return client{}, false
}

func (sm *ShardMaster) copyClientTable() map[string]client {
	sm.ctMu.RLock()
	defer sm.ctMu.RUnlock()

	copiedClientTable := map[string]client{}
	for k, v := range sm.clientTable {
		c := *v
		copiedClientTable[k] = c
	}

	return copiedClientTable
}

func (sm *ShardMaster) installClientTable(ct map[string]client) {
	sm.ctMu.Lock()
	defer sm.ctMu.Unlock()

	for k, c := range ct {
		sm.clientTable[k] = &client{
			ClientID:          c.ClientID,
			SeqNum:            c.SeqNum,
			AppliedInd:        c.AppliedInd,
			AppliedTerm:       c.AppliedTerm,
			LastExecutedValue: c.LastExecutedValue,
		}
	}

	sm.printf("installing client table: %v", sm.clientTable)
}

func (sm *ShardMaster) startRequest(args *Args, reply *Reply) (raftLogInd int, success bool) {
	cID := args.ClientID
	seqNum := args.SeqNum

	c, ok := sm.getClient(cID)
	if ok {
		if c.SeqNum > seqNum {
			// stale request
			// discard
			sm.printf("Stale req: %v, client data in table: %v", args, c)
			return -1, false
		}

		if c.SeqNum == seqNum {
			// successs request
			// reply.SetTime(time.Now().UnixNano())

			reply.err = OK
			if args.opType == queryType {
				config, _ := c.LastExecutedValue.(Config)
				reply.config = config
			}

			sm.printf("Handled req: %v, reply", args, c.LastExecutedValue)
			return -1, false
		}
	}

	cmd := Op{
		Type:     args.opType,
		Value:    args.value,
		ClientID: cID,
		SeqNum:   seqNum,
	}

	sm.printf("Start: %v", cmd)
	ind, _, ok := sm.rf.Start(cmd)

	if !ok {
		reply.wrongLeader = true
		// reply.SetTime(time.Now().UnixNano())
		sm.printf("I am no leader: %v : %v", args, reply)
		return -1, false
	}

	return ind, true
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sm.printf("join args: %v, reply: %v", args, reply)
	cID := args.ClientID
	seqNum := args.SeqNum

	r := &Reply{}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(args.Servers)
	data := w.Bytes()

	ind, ok := sm.startRequest(&Args{
		Header: Header{
			ClientID: cID,
			SeqNum:   seqNum,
		},
		opType: joinType,
		value:  data,
	}, r)
	if !ok {
		// request is handled or stale
		reply.WrongLeader = r.wrongLeader
		reply.Err = r.err

		sm.printf("Dont handle %v : %v", args, reply)
		return
	}

	done := make(chan Op)
	sm.jobTable.AtomicOp(fmt.Sprintf("%v", ind), sm.appendJobWrapper(job{
		ind: ind,
		op: Op{
			Type:     joinType,
			ClientID: cID,
			SeqNum:   seqNum,
		},
		done: done,
	}))

	opDone := <-done
	// reply.Time = time.Now().UnixNano()
	if opDone.Type != joinType || opDone.ClientID != cID || opDone.SeqNum != seqNum {
		reply.Err = FAIL
		sm.printf("%v:%v failed", cID, seqNum)
		return
	}

	reply.Err = OK
	sm.printf("%v:%v finished", cID, seqNum)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sm.printf("leave args: %v, reply: %v", args, reply)
	cID := args.ClientID
	seqNum := args.SeqNum

	r := &Reply{}
	ind, ok := sm.startRequest(&Args{
		Header: Header{
			ClientID: cID,
			SeqNum:   seqNum,
		},
		opType: leaveType,
		value:  args.GIDs,
	}, r)
	if !ok {
		// request is handled or stale
		reply.WrongLeader = r.wrongLeader
		reply.Err = r.err

		sm.printf("Dont handle %v : %v", args, reply)
		return
	}

	done := make(chan Op)
	sm.jobTable.AtomicOp(fmt.Sprintf("%v", ind), sm.appendJobWrapper(job{
		ind: ind,
		op: Op{
			Type:     leaveType,
			ClientID: cID,
			SeqNum:   seqNum,
		},
		done: done,
	}))

	opDone := <-done
	// reply.Time = time.Now().UnixNano()
	if opDone.Type != leaveType || opDone.ClientID != cID || opDone.SeqNum != seqNum {
		reply.Err = FAIL
		sm.printf("%v:%v failed", cID, seqNum)
		return
	}

	reply.Err = OK
	sm.printf("%v:%v finished", cID, seqNum)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sm.printf("move args: %v, reply: %v", args, reply)
	cID := args.ClientID
	seqNum := args.SeqNum

	r := &Reply{}
	ind, ok := sm.startRequest(&Args{
		Header: Header{
			ClientID: cID,
			SeqNum:   seqNum,
		},
		opType: moveType,
		value:  args.ShardGIDPair,
	}, r)
	if !ok {
		// request is handled or stale
		reply.WrongLeader = r.wrongLeader
		reply.Err = r.err

		sm.printf("Dont handle %v : %v", args, reply)
		return
	}

	done := make(chan Op)
	sm.jobTable.AtomicOp(fmt.Sprintf("%v", ind), sm.appendJobWrapper(job{
		ind: ind,
		op: Op{
			Type:     moveType,
			ClientID: cID,
			SeqNum:   seqNum,
		},
		done: done,
	}))

	opDone := <-done
	// reply.Time = time.Now().UnixNano()
	if opDone.Type != moveType || opDone.ClientID != cID || opDone.SeqNum != seqNum {
		reply.Err = FAIL
		sm.printf("%v:%v failed", cID, seqNum)
		return
	}

	reply.Err = OK
	sm.printf("%v:%v finished", cID, seqNum)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sm.printf("query args: %v, reply: %v", args, reply)
	cID := args.ClientID
	seqNum := args.SeqNum

	r := &Reply{}
	ind, ok := sm.startRequest(&Args{
		Header: Header{
			ClientID: cID,
			SeqNum:   seqNum,
		},
		opType: queryType,
		value:  args.Num,
	}, r)
	if !ok {
		// request is handled or stale
		reply.WrongLeader = r.wrongLeader
		reply.Err = r.err
		reply.Config = r.config

		sm.printf("Dont handle %v : %v", args, reply)
		return
	}

	done := make(chan Op)
	sm.jobTable.AtomicOp(fmt.Sprintf("%v", ind), sm.appendJobWrapper(job{
		ind: ind,
		op: Op{
			Type:     queryType,
			ClientID: cID,
			SeqNum:   seqNum,
			Value:    args.Num,
		},
		done: done,
	}))

	opDone := <-done
	// reply.Time = time.Now().UnixNano()
	if opDone.Type != queryType || opDone.ClientID != cID || opDone.SeqNum != seqNum {
		reply.Err = FAIL
		sm.printf("%v:%v failed", cID, seqNum)
		return
	}

	config, _ := opDone.Value.(Config)
	reply.Err = OK
	reply.Config = config

	sm.printf("%v:%v finished", cID, seqNum)
}

func (sm *ShardMaster) appendJobWrapper(j job) func(interface{}) interface{} {
	return func(v interface{}) interface{} {
		val, ok := v.([]job)
		if !ok {
			return []job{j}
		}

		return append(val, j)
	}
}

func (sm *ShardMaster) applyJoin(servers map[int][]string) {
	c := sm.configs[len(sm.configs)-1]
	newG := map[int][]string{}

	for id, members := range c.Groups {
		newG[id] = members
	}

	for id, members := range servers {
		newG[id] = members
	}

	oldGIDToShards := map[int][]int{}
	for shard, gid := range c.Shards {
		oldGIDToShards[gid] = append(oldGIDToShards[gid], shard)
	}

	shardsShouldMove := oldGIDToShards[0]
	delete(oldGIDToShards, 0)

	maxShardNumPerGroup := int64(math.Ceil(float64(NShards) / float64(len(newG))))
	minShardNumPerGroup := int64(math.Floor(float64(NShards) / float64(len(newG))))
	allocatedShards := int64(0)
	for gid, shards := range oldGIDToShards {
		if int64(len(shards)) <= minShardNumPerGroup {
			allocatedShards += int64(len(shards))
			continue
		}

		if int64(len(shards)) == maxShardNumPerGroup {
			if allocatedShards+int64(len(shards)) <= int64(NShards) {
				allocatedShards += int64(len(shards))
				continue
			}

			shardsShouldMove = append(shardsShouldMove, shards[minShardNumPerGroup:]...)
			oldGIDToShards[gid] = shards[:minShardNumPerGroup]
			allocatedShards += int64(len(oldGIDToShards[gid]))
			continue
		}

		shardsShouldMove = append(shardsShouldMove, shards[maxShardNumPerGroup:]...)
		oldGIDToShards[gid] = shards[:maxShardNumPerGroup]
		allocatedShards += int64(len(oldGIDToShards[gid]))
	}

	newGIDToShards := map[int][]int{}
	for gid := range servers {
		newGIDToShards[gid] = shardsShouldMove[:minShardNumPerGroup]
		shardsShouldMove = shardsShouldMove[minShardNumPerGroup:]
	}

	newConfig := Config{
		Groups: newG,
		Num:    c.Num + 1,
	}

	for gid, shards := range oldGIDToShards {
		for _, shard := range shards {
			newConfig.Shards[shard] = gid
		}
	}

	for gid, shards := range newGIDToShards {
		for _, shard := range shards {
			newConfig.Shards[shard] = gid
		}
	}

	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) applyLeave(gids []int) {
	c := sm.configs[len(sm.configs)-1]
	newG := map[int][]string{}

	for id, members := range c.Groups {
		newG[id] = members
	}

	GIDToShards := map[int][]int{}
	for shard, gid := range c.Shards {
		GIDToShards[gid] = append(GIDToShards[gid], shard)
	}

	shardsShouldMove := []int{}
	for _, gid := range gids {
		shardsShouldMove = append(shardsShouldMove, GIDToShards[gid]...)
		delete(newG, gid)
		delete(GIDToShards, gid)
	}

	shardInd := 0
	for shardInd < len(shardsShouldMove) {
		for gid := range GIDToShards {
			if shardInd >= len(shardsShouldMove) {
				break
			}

			GIDToShards[gid] = append(GIDToShards[gid], shardsShouldMove[shardInd])
			shardInd++
		}
	}

	newConfig := Config{
		Groups: newG,
		Num:    c.Num + 1,
	}

	for gid, shards := range GIDToShards {
		for _, shard := range shards {
			newConfig.Shards[shard] = gid
		}
	}

	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) applyMove(pair ShardGIDPair) {
	c := sm.configs[len(sm.configs)-1]

	var newShards [NShards]int
	for shard, gid := range c.Shards {
		newShards[shard] = gid
	}

	newShards[pair.Shard] = pair.GID
	sm.configs = append(sm.configs, Config{
		Num:    c.Num + 1,
		Groups: c.Groups,
		Shards: newShards,
	})
}

func (sm *ShardMaster) apply(msg raft.ApplyMsg) {
	cmd, ok := msg.Command.(Op)
	if !ok {
		log.Printf("Invalid cmd: %v, discard\n", msg.Command)
		return
	}

	c, ok := sm.getClient(cmd.ClientID)
	if !ok || c.SeqNum < cmd.SeqNum || (c.SeqNum == cmd.SeqNum && cmd.Type == queryType) {
		switch cmd.Type {
		case queryType:
			if ok && c.SeqNum == cmd.SeqNum {
				cmd.Value = c.LastExecutedValue
				break
			}

			num, _ := cmd.Value.(int)
			if num == -1 || num >= len(sm.configs) {
				cmd.Value = sm.configs[len(sm.configs)-1]
				break
			}

			cmd.Value = sm.configs[num]
		case joinType:
			val, _ := cmd.Value.([]byte)

			r := bytes.NewBuffer(val)
			d := labgob.NewDecoder(r)
			var servers map[int][]string
			if d.Decode(&servers) != nil {
				log.Fatalf("%d decode join map error", sm.me)
			}

			sm.applyJoin(servers)
			cmd.Value = servers
		case leaveType:
			gids, _ := cmd.Value.([]int)
			sm.applyLeave(gids)
		case moveType:
			pair, _ := cmd.Value.(ShardGIDPair)
			sm.applyMove(pair)
		default:
			log.Printf("Invalid cmd type: %v, discard\n", cmd.Type)
			return
		}
	}

	sm.setClient(&client{
		ClientID:          cmd.ClientID,
		SeqNum:            cmd.SeqNum,
		AppliedInd:        msg.CommandIndex,
		AppliedTerm:       msg.CommandTerm,
		LastExecutedValue: cmd.Value,
	})

	cmdInd := fmt.Sprintf("%v", msg.CommandIndex)
	entry, ok := sm.jobTable.Get(cmdInd)
	if !ok {
		return
	}

	jobs, _ := entry.([]job)
	sm.printf("JOBS: %v", jobs)

	for _, job := range jobs {
		job.done <- cmd
		close(job.done)
	}

	sm.jobTable.Delete(cmdInd)
}

func (sm *ShardMaster) cleanUpSatleReq() {
	appliedInd := sm.getAppliedInd()
	for ind := int64(1); ind < appliedInd; ind++ {
		cmdInd := fmt.Sprintf("%v", ind)
		entry, ok := sm.jobTable.Get(cmdInd)
		if !ok {
			continue
		}

		jobs, _ := entry.([]job)
		for _, job := range jobs {
			close(job.done)
		}

		sm.jobTable.Delete(cmdInd)
	}
}

func (sm *ShardMaster) snapshot(indexInLog, cmdInd, term int) {
	if sm.maxraftstate == -1 || sm.maxraftstate > sm.rf.GetPersistentSize() {
		return
	}

	clientTable := sm.copyClientTable()
	sm.printf("client table to be snapshoted: %v", clientTable)
	sm.printf("configs to be snapshoted: %v", sm.configs)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(sm.configs)
	e.Encode(clientTable)
	data := w.Bytes()
	sm.rf.Snapshot(data, indexInLog, cmdInd, term)
}

func (sm *ShardMaster) restoreStateFromSnapshot(msg raft.ApplyMsg) {
	snapshot, ok := msg.Command.([]byte)
	if !ok {
		log.Fatalf("%v failed to restore from snapshot", sm.me)
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()
	if snapshot == nil || len(snapshot) < 1 || int64(msg.CommandIndex) < sm.appliedInd {
		sm.printf("reject installsnapshot: %v", msg)
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var configs []Config
	var clientTable map[string]client

	if d.Decode(&configs) != nil || d.Decode(&clientTable) != nil {
		log.Fatalf("%d restore failed", sm.me)
	}

	sm.printf("instal lsnapshot go go: %v", msg)
	sm.printf("original store: %v", sm.configs)
	sm.configs = configs
	sm.printf("new store: %v", configs)

	sm.printf("original client table: %v", sm.clientTable)
	sm.printf("new client table: %v", clientTable)
	sm.installClientTable(clientTable)
	sm.updateAppliedInd(int64(msg.CommandIndex))
	sm.updateAppliedTerm(int64(msg.CommandTerm))
}

func (sm *ShardMaster) processApplyMsg(msg raft.ApplyMsg) {
	switch msg.Type {
	case raft.StateMachineCmdEntry:
		appliedInd := sm.getAppliedInd()
		if appliedInd > int64(msg.CommandIndex) {
			return
		}

		sm.apply(msg)
		sm.updateAppliedInd(int64(msg.CommandIndex))
		if int64(msg.CommandTerm) > sm.getAppliedTerm() {
			sm.updateAppliedTerm(int64(msg.CommandTerm))
		}

		sm.snapshot(msg.IndexInLog, msg.CommandIndex, msg.CommandTerm)

	case raft.SnapshotEntry:
		sm.restoreStateFromSnapshot(msg)

	case raft.TermEntry:
		if int64(msg.CommandTerm) > sm.getAppliedTerm() {
			sm.updateAppliedTerm(int64(msg.CommandTerm))
		}
	}
}

func (sm *ShardMaster) startLoop() {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for !sm.killed() {
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
				sm.cleanUpSatleReq()
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
			case applyMsg := <-sm.applyCh:
				sm.printf("applyMsg: %v", applyMsg)
				sm.processApplyMsg(applyMsg)
			}
		}
	}()
}

//
// for debug
//
func (sm *ShardMaster) printf(format string, a ...interface{}) {
	a = append([]interface{}{sm.me}, a...)
	DPrintf("%v: "+format, a...)
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.rf.Kill()
	// Your code here, if desired.
}

func (sm *ShardMaster) killed() bool {
	z := atomic.LoadInt32(&sm.dead)
	return z == 1
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	labgob.Register(Op{})
	applyCh := make(chan raft.ApplyMsg)
	sm := &ShardMaster{
		me:           me,
		maxraftstate: -1,
		configs: []Config{
			Config{
				Groups: map[int][]string{},
			},
		},
		applyCh:     applyCh,
		rf:          raft.Make(servers, me, persister, applyCh),
		clientTable: make(map[string]*client),
		jobTable: KVStore{
			kvTable: make(map[string]interface{}),
		},
	}

	// Your code here.
	sm.startLoop()

	return sm
}
