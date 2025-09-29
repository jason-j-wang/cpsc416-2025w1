package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"time"
)

// Additional imports

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	workerId := time.Now().UnixNano()
	args := WorkerArgs{}
	args.WorkerId = workerId
	RegisterWorker(args)
	go CallHeartbeat(workerId)

	for {
		reply := TaskReply{}

		ok := call("Coordinator.Task", &args, &reply)
		if !ok {
			break
		}

		if reply.JobId == -1 {
			time.Sleep((time.Second))
			continue
		}

		compArgs := CompletionArgs{
			JobId:     0,
			JobType:   "map",
			WorkderId: workerId,
		}
		compReply := GenericReply{}
		call("Coordinator.WorkerCompletion", &compArgs, &compReply)
		time.Sleep(time.Second)

		// TODO
		// 1. If map task, execute map function
		// 2. If reduce task, execute reduce function
		// 3. report completion to coodinator in RPC call
	}

}

// Ping the coordinator every second
func CallHeartbeat(workerId int64) {
	for {
		args := WorkerArgs{}
		reply := GenericReply{}

		ok := call("Coordinator.HeartbeatRPC", &args, &reply)
		if ok {

		} else {
			fmt.Printf("call failed!\n")
		}

		time.Sleep(1 * time.Second)
	}
}

func RegisterWorker(args WorkerArgs) {
	reply := GenericReply{}

	ok := call("Coordinator.RegisterWorkerRPC", &args, &reply)
	if ok {

	} else {
		fmt.Printf("call failed!\n")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
