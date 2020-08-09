package mr

import (
	"context"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	timeout    = time.Duration(10 * time.Second)
	reduceBase = 30678
	done       = -1
	delimiter  = "-"
)

// Master is a struct that stores information for master server
type Master struct {
	// Your definitions here.
	lock           *sync.RWMutex
	works          map[int]work
	files          []string
	fileIndChan    chan int
	mapDoneNum     int
	reduceDoneNum  int
	nReduce        int
	reduceFileName map[int][]string
	wg             *sync.WaitGroup
}

type work struct {
	jobType   jobType
	id        int
	done      bool
	fail      bool
	timestamp int64
}

// Your code here -- RPC handlers for the worker to call.

//
// getFileInd returns file name index for map processing
//
func (m *Master) getFileInd() int {
	return <-m.fileIndChan
}

//
// Register function for workers to register themself
//
func (m *Master) Register(args *RegisterArgs, reply *RegisterReply) error {
	ts := time.Now().UnixNano()

	id := m.getFileInd()

	m.lock.Lock()
	defer m.lock.Unlock()

	if m.nReduce == m.reduceDoneNum {
		reply.JobType = jobDone
		return nil
	}

	jobType := mapJob
	if id >= reduceBase {
		id -= reduceBase
		jobType = reduceJob
		reply.FileNames = m.reduceFileName[id]
	} else {
		reply.FileNames = []string{m.files[id]}
	}

	m.works[id] = work{
		jobType:   jobType,
		id:        id,
		timestamp: ts,
		done:      false,
		fail:      false,
	}

	reply.JobType = jobType
	reply.ID = id
	reply.Timestamp = ts
	reply.NReduce = m.nReduce

	return nil
}

//
// Finish function for workers to notify master that
// they have done the job
//
func (m *Master) Finish(args *FinishArgs, reply *FinishReply) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	if args.JobType == mapJob {
		m.mapDoneNum++

		// intermediate files
		for _, fileName := range args.FileNames {
			splitted := strings.Split(fileName, delimiter)
			reduceID, _ := strconv.Atoi(splitted[len(splitted)-1])
			m.reduceFileName[reduceID] = append(m.reduceFileName[reduceID], fileName)
		}

		if m.mapDoneNum == len(m.files) {
			// all map jobs are finished
			go func() {
				for i := 0; i < m.nReduce; i++ {
					m.fileIndChan <- (i + reduceBase)
				}
			}()

			// clear map job map works
			for k := range m.works {
				delete(m.works, k)
			}
		}
	} else if args.JobType == reduceJob {
		m.reduceDoneNum++
		if m.reduceDoneNum == m.nReduce {
			// all reduce jobs are finished
			close(m.fileIndChan)
			m.wg.Done()
		}
	}

	reply.Timestamp = time.Now().UnixNano()
	w := m.works[args.ID]
	w.done = true
	m.works[args.ID] = w

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (m *Master) checkWorkStatus() {
	m.lock.Lock()
	defer m.lock.Unlock()
	now := time.Now()
	for id, work := range m.works {
		if work.done {
			continue
		}

		if work.jobType == reduceJob {
			id += reduceBase
		}

		t := time.Unix(work.timestamp, 0)
		expiredTime := t.Add(timeout)
		if now.After(expiredTime) {
			// failed, reassign job
			log.Printf("ID: %v failed", id)
			m.fileIndChan <- id
		}
	}
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.wg.Add(1)
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				m.checkWorkStatus()
				time.Sleep(time.Second)
			}
		}
	}(ctx)

	m.wg.Wait()
	return true
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	// Your code here.
	m := Master{
		lock:           &sync.RWMutex{},
		works:          map[int]work{},
		mapDoneNum:     0,
		reduceDoneNum:  0,
		files:          files,
		fileIndChan:    make(chan int),
		nReduce:        nReduce,
		reduceFileName: map[int][]string{},
		wg:             &sync.WaitGroup{},
	}

	go func() {
		for ind := range files {
			m.fileIndChan <- ind
		}
	}()

	m.server()
	return &m
}
