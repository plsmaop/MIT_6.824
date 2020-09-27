package shardmaster

import "log"

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10
const debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

type Err string

const (
	OK   Err = "OK"
	FAIL Err = "FAIL"
)

type opType string

const (
	joinType  opType = "JOIN"
	leaveType opType = "LEAVE"
	moveType  opType = "MOVE"
	queryType opType = "QUERY"
)

type Header struct {
	ClientID string
	SeqNum   int64
	Time     int64
}

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	Header
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs []int
	Header
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type ShardGIDPair struct {
	Shard int
	GID   int
}

type MoveArgs struct {
	ShardGIDPair
	Header
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number
	Header
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

type Args struct {
	Header
	opType opType
	value  interface{}
}

type Reply struct {
	wrongLeader bool
	err         Err
	config      Config
}
