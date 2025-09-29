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

type WorkerArgs struct {
	WorkerId int64
}

type GenericReply struct {
	Success bool
}

type TaskReply struct {
	JobId   int
	File    string
	NReduce int
	//probs more
}

type CompletionArgs struct {
	JobId     int
	JobType   string
	WorkderId int64
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
func coordinatorSock() string {
	s := "/var/tmp/416-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
