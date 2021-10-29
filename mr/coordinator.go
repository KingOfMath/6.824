package mr

import (
	"log"
	"strconv"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// 锁
	mu sync.Mutex
	// 是否完成
	mapAllDone    bool
	reduceAllDone bool
	// 任务
	mapTasks    []TaskState
	reduceTasks []TaskState
	// map/reduce个数
	mMap    int
	nReduce int
}

// RpcHandler Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RpcHandler(args *RpcArgs, reply *RpcReply) error {
	taskType := args.taskState.TaskType
	switch taskType {

	}

	return nil
}

// 创建线程监听RPC
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done 返回所有任务是否完成
func (c *Coordinator) Done() bool {

	c.mapAllDone = c.checkAllMapTasks()
	c.reduceAllDone = c.checkAllReduceTasks()
	return c.mapAllDone && c.reduceAllDone
}

func (c *Coordinator) checkAllMapTasks() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, v := range c.mapTasks {
		if v.State != StateDone {
			return false
		}
	}
	return true
}

func (c *Coordinator) checkAllReduceTasks() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, v := range c.reduceTasks {
		if v.State != StateDone {
			return false
		}
	}
	return true
}

// MakeCoordinator 创建master
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.mMap = len(files)
	c.nReduce = nReduce

	c.mapTasks = []TaskState{}
	c.reduceTasks = []TaskState{}
	for i := 0; i < c.mMap; i++ {
		task := TaskState{
			State:    StateIdle,
			TaskName: files[i],
			TaskType: MapTask,
		}
		c.mapTasks = append(c.mapTasks, task)
	}
	for i := 0; i < c.nReduce; i++ {
		task := TaskState{
			State:    StateIdle,
			TaskName: "Reduce " + strconv.Itoa(i),
			TaskType: ReduceTask,
		}
		c.reduceTasks = append(c.reduceTasks, task)
	}

	c.server()
	return &c
}
