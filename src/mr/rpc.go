package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

type RequestTask struct {
}

type RequestTaskState int

const (
	IDLE RequestTaskState = iota
	MAP
	REDUCE
	COMPLETE
)

type RequestTaskReply struct {
	rtype    RequestTaskState
	key      int
	filename string
	nReducer int
	idleTime time.Duration
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
