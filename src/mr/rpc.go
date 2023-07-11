package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

const (
	//_timeout = -1 worker timeout
	_map    = 0
	_reduce = 1
	_wait   = 2
	_end    = 3
	_failed = 4
)

// master 和 worker之间,是通过rpc进行通信的
type TaskState struct {
	/*
		Declared in consts above
			0  map
			1  reduce
			2  wait
			3  end
	*/
	State       int
	MapIndex    int
	ReduceIndex int
	FileName    string
	R           int
	M           int
}

func (ts *TaskState) Log(prefix string) {
	fmt.Printf("%s taskState: %+v\n", prefix, ts)
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
