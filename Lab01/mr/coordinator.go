package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

// Additional imports
import "time"

type Coordinator struct {
	// Your definitions here.
	Workers []WorkerData

}

type WorkerData struct {
	WorkerId string

	// "idle", "busy", "crashed"
	Status string

	// "map", "reduce"
	JobType string

	// Time when the worker started its current task, -1 if idle
	JobStartTime int

	// Time since last heartbeat received
	LastHeartbeat int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RegisterWorker(workerId string, reply *GenericReply) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	// Add worker to list of workers
	worker := WorkerData{}
	worker.WorkerId = workerId
	worker.Status = "idle"
	worker.JobStartTime = -1
	worker.LastHeartbeat = time.Now().Unix()
	c.Workers = append(c.Workers, worker)

	reply.Success = true
	return nil;
}

func (c *Coordinator) HeartbeatMonitor(workerID string) {
	return nil;
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.


	c.server()
	return &c
}
