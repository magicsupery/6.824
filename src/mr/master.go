package mr

import (
	"github.com/vmihailenco/msgpack"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Phase int
const (
	INIT Phase = iota
	Map
	Reduce
	Exit
)

type Master struct {
	// Your definitions here.
	rwLock sync.RWMutex
	fileToTimestamp map[string]int64
	tmpSn uint32
	tmpFiles []string

	nReduce int
	reduceToTimestamp map[int]int64
	curPhase Phase
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) ApplyTask(args *ApplyTaskArgs, reply *ApplyTaskReply) error{
	m.rwLock.Lock()
	defer m.rwLock.Unlock()

	if m.curPhase == Map{
		// find avaiable file
		now := time.Now().Unix()
		resName := ""
		for name, last := range m.fileToTimestamp{
			if now - last > 10{
				resName = name
				m.fileToTimestamp[name] = now
				break
			}
		}
		if resName == ""{
			reply.Task = NoTask
		}else{
			reply.Task = MapTask
			mapArgs := MapArgs{}
			mapArgs.FileName = resName
			mapArgs.Sn = m.tmpSn
			m.tmpSn ++
			reply.Args, _ = msgpack.Marshal(mapArgs)
		}
	}else if m.curPhase == Reduce{
		now := time.Now().Unix()
		resReduce := -1

		for reduce, last := range m.reduceToTimestamp{
			if now - last > 10{
				resReduce = reduce
				m.reduceToTimestamp[reduce] = now
				break
			}
		}

		if resReduce == -1{
			reply.Task = NoTask
		}else{
			reply.Task = ReduceTask
			reduceArgs := ReduceArgs{}
			reduceArgs.Reduce = resReduce
			reduceArgs.Nreduce = m.nReduce
			reduceArgs.TmpFileNames = m.tmpFiles

			reply.Args, _ = msgpack.Marshal(reduceArgs)

		}

	}else if m.curPhase == Exit{
		reply.Task = ExitTask
	}
	return nil
}

func (m *Master) MapComplete(args *MapCompleteArgs, reply *MapCompleteReply) error{
	m.rwLock.Lock()
	defer m.rwLock.Unlock()

	// one Task maybe give two workers, need check
	if _, ok := m.fileToTimestamp[args.FileName]; ok{
		delete(m.fileToTimestamp, args.FileName)
		m.tmpFiles = append(m.tmpFiles, args.TmpFileName)
		if len(m.fileToTimestamp) == 0{
			m.curPhase = Reduce
		}
	}

	return nil
}

func (m *Master) ReduceComplete(args *ReduceCompleteArgs, reply *ReduceCompleteReply) error{
	m.rwLock.Lock()
	defer m.rwLock.Unlock()

	if _, ok := m.reduceToTimestamp[args.Reduce]; ok{
		delete(m.reduceToTimestamp, args.Reduce)
		if len(m.reduceToTimestamp) == 0{
			m.curPhase = Exit
		}
	}
	return nil
}
//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()

	os.Remove(sockname)

	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.
	m.rwLock.RLock()
	defer m.rwLock.RUnlock()

	return m.curPhase == Exit
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of Reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		fileToTimestamp: make(map[string]int64),
		tmpSn: 0,
		tmpFiles: []string{},
		nReduce:nReduce,
		reduceToTimestamp: make(map[int]int64),
		curPhase: INIT,
	}

	for _, name := range files{
		m.fileToTimestamp[name] = time.Now().Unix()
	}

	i := 0
	for i < nReduce{
		m.reduceToTimestamp[i] = time.Now().Unix()
		i++
	}

	// Your code here.
	m.curPhase = Map
	m.server()
	return &m
}
