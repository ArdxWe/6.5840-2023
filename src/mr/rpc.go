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
type JobType int8

const (
	MapJobType    = JobType(0)
	ReduceJobType = JobType(1)
)

type MapJobInfo struct {
	Index         int64
	InputFileName string
	ReduceCount   int64
}

type ReduceJobInfo struct {
	Index    int64
	MapCount int64
}

type AssignJobRequest struct {
}

type AssignJobResponse struct {
	JobType   JobType
	MapJob    MapJobInfo
	ReduceJob ReduceJobInfo

	Finish bool
}

type FinishJobRequest struct {
	Index   int64
	JobType JobType
}

type FinishJobResponse struct {
	Finish bool
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
