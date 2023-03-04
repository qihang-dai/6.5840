package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
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

type TaskType int

const (
	// for worker report task status in REQUEST
	INIT = iota
	DONE
	// for coordinator to assign task to worker
	MAP
	REDUCE
	WAIT
)

//request task from coordinator. also indicate the task status. since args communicate coordinator and worker in both directions, 
//Files is used to send the processed file content to the coordinator but may looks useless when worker request task from coordinator
type TaskArgs struct {
	Type TaskType
	Id  int
	Files []string // when worker report task status, it will send the processed files to the coordinator
}

type TaskReply struct {
	Type TaskType
	Id  int
	NReduce int
	Files []string //assign single file name to map task in Files[0], assign intermediate file names to reduce tasks 
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
