package kvraft

import "time"

type Err string

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrUnknown     = "ErrUnknown"
)

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ID   string
	Time time.Time
}

type PutAppendReply struct {
	Err      Err
	LeaderID int
	ID       string
	Time     time.Time
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ID   string
	Time time.Time
}

type GetReply struct {
	Err      Err
	Value    string
	LeaderID int
	ID       string
	Time     time.Time
}
