package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.

type jobType int

const (
	jobDone jobType = iota
	mapJob
	reduceJob
)

type RegisterArgs struct {
	Timestamp int64
}

type RegisterReply struct {
	ID        int
	Timestamp int64
	FileNames []string
	JobType   jobType
	NReduce   int
}

type FinishArgs struct {
	JobType   jobType
	ID        int
	Timestamp int64
	// intermediate files
	FileNames []string
}

type FinishReply struct {
	Timestamp int64
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
