package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

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

		success := false
		if reply.JobType == "map" {
			success = executeMapTask(reply.JobId, reply.File, reply.NReduce, mapf)
		} else if reply.JobType == "reduce" {
			success = executeReduceTask(reply.JobId, reply.NMapTasks, reducef)
		}

		if success {
			compArgs := CompletionArgs{
				JobId:     reply.JobId,
				JobType:   reply.JobType,
				WorkderId: workerId,
			}
			compReply := GenericReply{}
			call("Coordinator.WorkerCompletion", &compArgs, &compReply)
		}
	}
}

func executeMapTask(jobId int, filename string, nReduce int, mapf func(string, string) []KeyValue) bool {
	// Same file reading as sequential version
	file, err := os.Open(filename)
	if err != nil {
		//log.Printf("MAP OPEN FILE FAILED");
		return false
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		//log.Printf("MAP READ FILE FAILED");
		file.Close()
		return false
	}
	file.Close()

	// Same as sequential, we still need to call the map function to get hte key value pairs
	kva := mapf(filename, string(content))

	// Here instead of storing in memory (as we did in seq), we start the process of creating files which are then accessed by reduce tasks
	// As suggested in the README we are going to use temp files, which we then rename at the end
	var intermediateFiles []*os.File
	var encoders []*json.Encoder
	
	// Create our temp files and jsonencoders (which we use to write the kv to the temp files)
	for reduceTask := 0; reduceTask < nReduce; reduceTask++ {
		tmpfile, err := os.CreateTemp("", "TEMP-MR-FILE")
		if err != nil {
			//log.Printf("MAP TEMP FILE CREATION FAILED")
			for i := 0; i < reduceTask; i++ {
				intermediateFiles[i].Close()
			}
			return false
		}
    	intermediateFiles = append(intermediateFiles, tmpfile)
    	encoders = append(encoders, json.NewEncoder(tmpfile))
	}

	// Here is where we write the kv to file via the jsonencoders
	for _, kv := range kva {
		// As instructed in the readme, we use ihash to determine which reduceTask to send this kv to
		// we do this modulo the number of reduce tasks since we need to be assigning it to a valid reduce task index
		reduceTask := ihash(kv.Key) % nReduce
		
		// We write the kv pair to the file using the JSON encoder
		err := encoders[reduceTask].Encode(&kv)
		if err != nil {
			//log.Printf("MAP TASK FAILED TO WRITE INTERMEDIATE FILE")
			for i := 0; i < nReduce; i++ {
				intermediateFiles[i].Close()
			}
			return false
		}
	}

	// As suggested in readme we change the rename the temp files
	for reduceTask := 0; reduceTask < nReduce; reduceTask++ {
		intermediateFiles[reduceTask].Close()
		mapOutputFileName := fmt.Sprintf("mr-%d-%d", jobId, reduceTask)
		err := os.Rename(intermediateFiles[reduceTask].Name(), mapOutputFileName)
		if err != nil {
			//log.Printf("MAP TASK FAILED RENAME TEMP FILE")
			return false
		}
	}

	return true
}

func executeReduceTask(jobId int, nMapTasks int, reducef func(string, []string) string) bool {
	// In the sequential version, reduce has all of the data in one place in memory, but here we need to stitch the output files of each map task's
	// for this reduce task together
	var kva []KeyValue
	
	// reading the relevant files and getting their data into one kv array
	for mapTask := 0; mapTask < nMapTasks; mapTask++ {
		mapOutputFileName := fmt.Sprintf("mr-%d-%d", mapTask, jobId)
		
		file, err := os.Open(mapOutputFileName)
		if err != nil {
			//log.Printf("REDUCE TASK FAILED READ mapOutputFile")
			return false
		}
		
		// use the json decoder code provided in the readme
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	// Now that we have stitched the relevant data back together we can sort the data same as in sequential
	sort.Sort(ByKey(kva))

	// here we are creating the output file, as suggested in README we are using temp files then renaming them
	// Outside of that change the logic is the same as sequential, except each reduce worker is creating a outfile (but this code is per worker, hence only 1 file is written here)
	oname := fmt.Sprintf("mr-out-%d", jobId)
	tmpfile, err := os.CreateTemp("", "TEMP-MR-OUTFILE*")
	if err != nil {
		//log.Printf("REDUCE TEMP FILE CREATION FAILED")
		return false
	}
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(tmpfile, "%v %v\n", kva[i].Key, output)
		
		i = j
	}
	tmpfile.Close()
	err = os.Rename(tmpfile.Name(), oname)
	if err != nil {
		//log.Printf("FAILED RENAMING")
		return false
	}

	return true
}

// Taken from sequential impl, for sorting by key
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
