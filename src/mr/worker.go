package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for write temp file
type tempWriter struct {
	enc  *json.Encoder
	file *os.File
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// doMapJob execute map function
//
func doMapJob(reply *RegisterReply, nReduce int, mapf func(string, string) []KeyValue) {
	// do the map job
	fileName := reply.FileNames[0]
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	kva := mapf(fileName, string(content))

	fileWriters := map[int]tempWriter{}
	// create tmp files
	for _, kv := range kva {
		k := ihash(kv.Key) % nReduce
		if writer, ok := fileWriters[k]; ok {
			writeTemp(&writer, &kv)
			continue
		}

		fileNameTemplate := fmt.Sprintf("temp-%d-%d-*", reply.ID, k)
		f, err := ioutil.TempFile("./", fileNameTemplate)
		if err != nil {
			log.Fatalf("cannot create temp file %v", fileName)
		}
		enc := json.NewEncoder(f)
		writer := tempWriter{
			enc:  enc,
			file: f,
		}
		fileWriters[k] = writer
		writeTemp(&writer, &kv)
	}

	// close and rename temfiles
	intermediateFileNames := []string{}
	for id, writer := range fileWriters {
		writer.file.Close()
		outputFileName := fmt.Sprintf("mr-%d-%d", reply.ID, id)
		if _, err := os.Stat(outputFileName); err == nil {
			e := os.Remove(outputFileName)
			if e != nil {
				log.Fatal(e)
			}
		}

		os.Rename(writer.file.Name(), outputFileName)
		if err != nil {
			log.Fatalf("%v", err)
		}
		intermediateFileNames = append(intermediateFileNames, outputFileName)
	}

	callFinish(reply.JobType, reply.ID, intermediateFileNames)
}

//
// writeTemp writes the intermediate output from map function
//
func writeTemp(w *tempWriter, kv *KeyValue) {
	err := w.enc.Encode(kv)
	if err != nil {
		log.Fatalf("write temp file err: %v", err)
	}
}

func callFinish(jobType jobType, id int, fileNames []string) {
	args := FinishArgs{
		Timestamp: time.Now().UnixNano(),
		JobType:   jobType,
		ID:        id,
		FileNames: fileNames,
	}

	reply := FinishReply{}

	call("Master.Finish", &args, &reply)
}

//
// doReduceJob execute reduce function
//
func doReduceJob(reply *RegisterReply, reducef func(string, []string) string) {
	fileNames := reply.FileNames
	intermediate := []KeyValue{}
	for _, fileName := range fileNames {
		f, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}

		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}

		f.Close()
	}

	sort.Sort(ByKey(intermediate))

	outputTempFileNameTemplate := fmt.Sprintf("temp-%d-*", reply.ID)
	f, err := ioutil.TempFile("./", outputTempFileNameTemplate)
	if err != nil {
		log.Fatalf("cannot create temp file %v", outputTempFileNameTemplate)
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(f, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	f.Close()
	outputFileName := fmt.Sprintf("mr-out-%d", reply.ID)
	if _, err := os.Stat(outputFileName); err == nil {
		e := os.Remove(outputFileName)
		if e != nil {
			log.Fatal(e)
		}
	}

	err = os.Rename(f.Name(), outputFileName)
	if err != nil {
		log.Fatalf("%v", err)
	}
	callFinish(reply.JobType, reply.ID, []string{})
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := RegisterArgs{
			Timestamp: time.Now().UnixNano(),
		}

		reply := RegisterReply{}
		if !call("Master.Register", &args, &reply) {
			return
		}

		switch reply.JobType {
		case mapJob:
			doMapJob(&reply, reply.NReduce, mapf)
		case reduceJob:
			doReduceJob(&reply, reducef)
		case jobDone:
			return
		}
	}
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
