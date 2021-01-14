package mr

import (
	"fmt"
	"github.com/vmihailenco/msgpack"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

import _ "github.com/vmihailenco/msgpack"


const (
	TempDir = "/var/tmp/6.824/"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the Reduce
// Task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	taskReply := ApplyTaskReply{}
	ret := true
	for ret{
		taskReply, ret = applyTask()
		if taskReply.Task == ExitTask{
			break
		}else if taskReply.Task == NoTask{
			time.Sleep(time.Second)
		}else if taskReply.Task == MapTask{
			ret = handleMap(taskReply.Args, mapf)
		}else if taskReply.Task == ReduceTask{
			ret = handleReduce(taskReply.Args, reducef)
		}
	}

	log.Printf("[info] worker done %d", os.Getuid())
	// uncomment to send the Example RPC to the master.
	// CallExample()

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func applyTask() (ApplyTaskReply, bool){
	args := ApplyTaskArgs{}
	reply := ApplyTaskReply{}
	ret := call("Master.ApplyTask", &args, &reply)

	return reply, ret
}

func handleMap(args []byte, mapf func(string, string) []KeyValue) bool{
	mapArgs := MapArgs{}
	err := msgpack.Unmarshal(args, &mapArgs)
	if err != nil {
		log.Printf("[error] handleMap got msgpack error %s", err.Error())
		return false
	}

	file, err := os.Open(mapArgs.FileName)
	if err != nil {
		log.Printf("[error] handleMap got file open error %s", err.Error())
		return false
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("[error] handleMap got file read error %s", err.Error())
		return false
	}
	file.Close()

	kva := mapf(mapArgs.FileName, string(content))
	// write to a tmp FileName
	bytes, err := msgpack.Marshal(kva)
	if err != nil {
		log.Printf("[error] handleMap got msgpack marshal error %s", err.Error())
		return false
	}

	tmpFile, err := ioutil.TempFile(TempDir, "map")
	if err != nil {
		log.Printf("[error] handleMap got tmp file error %s", err.Error())
		return false
	}
	_, err = tmpFile.Write(bytes)
	if err != nil {
		log.Printf("[error] handleMap got write tmp file error %s", err.Error())
		return false
	}

	tmpFile.Close()

	// rename to it
	tmpFileName := fmt.Sprintf("mr-tmp.%d", mapArgs.Sn)
	err = os.Rename(tmpFile.Name(), tmpFileName)
	if err != nil {
		log.Printf("[error] handleMap got rename error %s", err.Error())
		return false
	}

	completeArgs := MapCompleteArgs{}
	completeArgs.FileName = mapArgs.FileName
	completeArgs.TmpFileName = tmpFileName

	completeReply := MapCompleteReply{}
	return call("Master.MapComplete", &completeArgs, &completeReply)
}

func handleReduce(args []byte, reducef func(string, []string) string) bool{
	reduceArgs := ReduceArgs{}

	err := msgpack.Unmarshal(args, &reduceArgs)
	if err != nil {
		log.Printf("[error] handleReduce got msgpack unmarshal error %s", err.Error())
		return false
	}

	//read all tmp files got all kv
	resKV := make([]KeyValue, 0)

	for _, fileName := range reduceArgs.TmpFileNames {
		bytes, err := ioutil.ReadFile(fileName)
		if err != nil {
			log.Printf("[error] handleReduce got read file error %s", err.Error())
			return false
		}

		var tmpKV []KeyValue
		err = msgpack.Unmarshal(bytes, &tmpKV)
		if err != nil {
			log.Printf("[error] handleReduce got unmarshal error %s", err.Error())
			return false
		}
		for _, kv := range tmpKV{
			resKV = append(resKV, kv)
		}
	}

	sort.Sort(ByKey(resKV))
	i := 0
	reduce :=  reduceArgs.Reduce
	tmpFile, err := ioutil.TempFile(TempDir, "map")
	if err != nil {
		log.Printf("[error] handleReduce got tmp file error %s", err.Error())
		return false
	}
	for i < len(resKV){
		if ihash(resKV[i].Key) % reduceArgs.Nreduce == reduce{
			j := i + 1
			for j < len(resKV) && resKV[j].Key == resKV[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, resKV[k].Value)
			}
			output := reducef(resKV[i].Key, values)
			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(tmpFile, "%v %v\n", resKV[i].Key, output)
			i = j
		}else{
			i ++
		}
	}
	tmpFile.Close()

	err = os.Rename(tmpFile.Name(), fmt.Sprintf("mr-out-%d", reduce))
	if err != nil {
		log.Printf("[error] handleReduce got rename error %s", err.Error())
		return false
	}

	completeArgs := ReduceCompleteArgs{}
	completeArgs.Reduce = reduce
	completeReply := ReduceCompleteReply{}
	return call("Master.ReduceComplete", &completeArgs, &completeReply)
}
//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
