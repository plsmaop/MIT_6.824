package kvraft

const debug = 0

type Err int

const (
	OK Err = iota
	ErrNoKey
	ErrWrongLeader
	ErrDuplicate
	ErrFail
	ErrUnknown
)

type opType string

const (
	cleanUpOp  opType = "cleanUp"
	getType           = "get"
	putType           = "put"
	appendType        = "append"
)

type Args interface {
	GetClientID() string
	GetSeqNum() int64
	GetKey() string
	GetValue() string
	GetOp() opType
	GetTimestamp() int64
}

type Reply interface {
	GetErr() Err
	GetClientID() string
	GetSeqNum() int64
	GetTimestamp() int64
	GetValue() string
	SetErr(Err)
	SetValue(string)
	SetTime(int64)
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
	SeqNum   int64
	Time     int64
}

func (paa *PutAppendArgs) GetClientID() string {
	return paa.ClientID
}

func (paa *PutAppendArgs) GetSeqNum() int64 {
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
	SeqNum   int64
	ClientID string
	Time     int64
}

func (par *PutAppendReply) GetClientID() string {
	return par.ClientID
}

func (par *PutAppendReply) GetSeqNum() int64 {
	return par.SeqNum
}

func (par *PutAppendReply) GetErr() Err {
	return par.Err
}

func (par *PutAppendReply) GetValue() string {
	return ""
}

func (par *PutAppendReply) GetTimestamp() int64 {
	return par.Time
}

func (par *PutAppendReply) SetErr(e Err) {
	par.Err = e
}

func (par *PutAppendReply) SetValue(val string) {

}

func (par *PutAppendReply) SetTime(t int64) {
	par.Time = t
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID string
	SeqNum   int64
	Time     int64
}

func (ga *GetArgs) GetClientID() string {
	return ga.ClientID
}

func (ga *GetArgs) GetSeqNum() int64 {
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
	SeqNum   int64
	ClientID string
	Time     int64
}

func (gr *GetReply) GetClientID() string {
	return gr.ClientID
}

func (gr *GetReply) GetSeqNum() int64 {
	return gr.SeqNum
}

func (gr *GetReply) GetErr() Err {
	return gr.Err
}

func (gr *GetReply) GetValue() string {
	return gr.Value
}

func (gr *GetReply) GetTimestamp() int64 {
	return gr.Time
}

func (gr *GetReply) SetErr(e Err) {
	gr.Err = e
}

func (gr *GetReply) SetValue(val string) {
	gr.Value = val
}

func (gr *GetReply) SetTime(t int64) {
	gr.Time = t
}
