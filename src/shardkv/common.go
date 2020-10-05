package shardkv

import "../shardmaster"

const debug = 0

const (
	configClientTD = "CONFIG_CLIENTID"
	configKey      = "CONFIG_KEY"
)

type Err string

const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrWrongGroup  Err = "ErrWrongGroup"
	ErrDuplicate   Err = "ErrDuplicate"
	ErrFail        Err = "ErrFail"
	ErrUnknown     Err = "ErrUnknown"
)

type opType string

const (
	cleanUpOp  opType = "cleanUp"
	getType    opType = "get"
	putType    opType = "put"
	appendType opType = "append"
	configType opType = "config"
)

type Header struct {
	ClientID string
	SeqNum   int64
	Time     int64
}

type Args interface {
	GetKey() string
	GetValue() string
	GetOp() opType
	GetHeader() Header
}

type Reply interface {
	GetErr() Err
	GetValue() string
	GetHeader() Header
	SetErr(Err)
	SetValue(string)
	SetHeader(Header)
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    opType // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Header
}

func (paa *PutAppendArgs) GetHeader() Header {
	return paa.Header
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

type PutAppendReply struct {
	Err      Err
	LeaderID int
	Header
}

func (par *PutAppendReply) GetHeader() Header {
	return par.Header
}

func (par *PutAppendReply) GetErr() Err {
	return par.Err
}

func (par *PutAppendReply) GetValue() string {
	return ""
}

func (par *PutAppendReply) SetHeader(h Header) {
	par.Header = h
}

func (par *PutAppendReply) SetErr(e Err) {
	par.Err = e
}

func (par *PutAppendReply) SetValue(val string) {

}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Header
}

func (ga *GetArgs) GetHeader() Header {
	return ga.Header
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

type GetReply struct {
	Err      Err
	Value    string
	LeaderID int
	Header
}

func (gr *GetReply) GetHeader() Header {
	return gr.Header
}

func (gr *GetReply) GetErr() Err {
	return gr.Err
}

func (gr *GetReply) GetValue() string {
	return gr.Value
}

func (gr *GetReply) SetHeader(h Header) {
	gr.Header = h
}

func (gr *GetReply) SetErr(e Err) {
	gr.Err = e
}

func (gr *GetReply) SetValue(val string) {
	gr.Value = val
}

type ConfigArgs struct {
	Header
	Config []byte
}

func (ca *ConfigArgs) GetHeader() Header {
	return ca.Header
}

func (ca *ConfigArgs) GetKey() string {
	return configKey
}

func (ca *ConfigArgs) GetValue() string {
	return string(ca.Config)
}

func (ca *ConfigArgs) GetOp() opType {
	return configType
}

type ConfigReply struct {
	Header
	Err Err
}

func (cr *ConfigReply) GetHeader() Header {
	return cr.Header
}

func (cr *ConfigReply) GetErr() Err {
	return cr.Err
}

func (cr *ConfigReply) GetValue() string {
	return ""
}

func (cr *ConfigReply) SetHeader(h Header) {
	cr.Header = h
}

func (cr *ConfigReply) SetErr(e Err) {
	cr.Err = e
}

func (cr *ConfigReply) SetValue(val string) {

}

type ShardArgs struct {
	Header
	GID int
	shardmaster.Config
	ShardsToPull []int
	Servers      []string
}

type ShardReply struct {
	Header
	Err
	ConfigNum int
	Data      map[string]string
}
