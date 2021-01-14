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
type Task int
const (
	NoTask Task = iota
	MapTask
	ReduceTask
	ExitTask
)

type ApplyTaskArgs struct {

}

type ApplyTaskReply struct {
	Task Task
	Args []byte
}

type MapArgs struct{
	FileName string
	Sn       uint32
}

type MapCompleteArgs struct {
	FileName    string
	TmpFileName string
}

type MapCompleteReply struct {

}

type ReduceArgs struct {
	Reduce       int
	Nreduce      int
	TmpFileNames []string
}

type ReduceCompleteArgs struct {
	Reduce int
}

type ReduceCompleteReply struct{
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
