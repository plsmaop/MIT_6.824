package kvraft

type Err string

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrUnknown     = "ErrUnknown"
)

type opType int

const (
	getType opType = iota
	putType
	appendType
)

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    opType // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ID   string
	Time int64
}

type PutAppendReply struct {
	Err      Err
	LeaderID int
	ID       string
	Time     int64
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ID   string
	Time int64
}

type GetReply struct {
	Err      Err
	Value    string
	LeaderID int
	ID       string
	Time     int64
}
