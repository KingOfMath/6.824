package mr

import (
	"fmt"
	"io/ioutil"
	"os"
)
import "log"
import "net/rpc"
import "hash/fnv"

// KeyValue
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
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

// Worker
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	reply := WorkerCaller()
	switch reply.taskState.TaskType {
	case MapTask:
		doMapf(mapf, &reply)
	case ReduceTask:
		doReducef(reducef, &reply)
	}
}

func doMapf(mapf func(string, string) []KeyValue, reply *RpcReply) {
	filename := reply.taskState.TaskName
	file, _ := os.Open(filename)
	content, _ := ioutil.ReadAll(file)
	defer file.Close()
	kvs := mapf(filename, string(content))

}

func doReducef(reducef func(string, []string) string, reply *RpcReply) {

}

// WorkerCaller RPC方法
func WorkerCaller() RpcReply {

	// declare an argument structure.
	args := RpcArgs{}
	reply := RpcReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.RpcHandler", &args, &reply)
	fmt.Printf("reply.Y %v\n", reply.taskState)

	return reply
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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

func Partition(kvs []KeyValue, nReduce int) [][]KeyValue {
	hashKvs := make([][]KeyValue, nReduce)
	for _, kv := range kvs {
		v := ihash(kv.Key) % nReduce
		hashKvs[v] = append(hashKvs[v], kv)
	}
	return hashKvs
}
