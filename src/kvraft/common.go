package kvraft

type Err int

const (
	OK Err = iota
	ErrNoKey
	ErrWrongLeader
	ErrDuplicate
	ErrFail
	ErrUnknown
)

type opType int

const (
	getType opType = iota
	putType
	appendType
)

type Args interface {
	GetClientID() string
	GetSeqNum() int
	GetKey() string
	GetValue() string
	GetOp() opType
	GetTimestamp() int64
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    opType // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID string
	SeqNum   int
	Time     int64
}

func (paa *PutAppendArgs) GetClientID() string {
	return paa.ClientID
}

func (paa *PutAppendArgs) GetSeqNum() int {
	return paa.SeqNum
}

func (paa *PutAppendArgs) GetKey() string {
	return paa.Key
}

func (paa *PutAppendArgs) GetValue() string {
	return paa.Value
}

func (paa *PutAppendArgs) GetOp() opType {
	return paa.Op
}

func (paa *PutAppendArgs) GetTimestamp() int64 {
	return paa.Time
}

type PutAppendReply struct {
	Err      Err
	LeaderID int
	SeqNum   int
	ClientID string
	Time     int64
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID string
	SeqNum   int
	Time     int64
}

func (ga *GetArgs) GetClientID() string {
	return ga.ClientID
}

func (ga *GetArgs) GetSeqNum() int {
	return ga.SeqNum
}

func (ga *GetArgs) GetKey() string {
	return ga.Key
}

func (ga *GetArgs) GetValue() string {
	return ""
}

func (ga *GetArgs) GetOp() opType {
	return getType
}

func (ga *GetArgs) GetTimestamp() int64 {
	return ga.Time
}

type GetReply struct {
	Err      Err
	Value    string
	LeaderID int
	SeqNum   int
	ClientID string
	Time     int64
}
