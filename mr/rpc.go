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

// example to show how to declare the arguments
// and reply for an RPC.
type ExampleArgs struct {
	X int
}
type ExampleReply struct {
	Y int
}

// Two types of calls: One to request tasks and one to update the coordinator of done task
type TaskArgs struct {
	Task string
}

type TaskReply struct {
	Tasktype     string // Map or reduce
	Filename     string // File name to be read from
	Partitionnum int    // The partition number of task
	Nreduce      int    //Nreduce
}

type TaskConfirmedArgs struct {
	Tasktype     string //map or reduce
	Partitionnum int    //which file number is done

}
type TaskConfirmedReply struct {
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
