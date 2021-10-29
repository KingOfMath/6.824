package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// 状态
const (
	StateIdle = iota
	StateWorking
	StateDone
)

// 类型
const (
	MapTask = iota
	ReduceTask
)

// Add your RPC definitions here.
type TaskState struct {
	State    int
	TaskType int
	TaskName string
}

type RpcArgs struct {
	taskState TaskState
}

type RpcReply struct {
	taskState TaskState
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
