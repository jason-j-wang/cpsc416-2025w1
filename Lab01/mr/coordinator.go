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
	workers map[string]WorkerData
	mapJobs []Job
}

type WorkerData struct {
	// "idle", "busy", "crashed"
	status string

	// "map", "reduce"
	curJobType string

	curJobId string

	// Time when the worker started its current task, -1 if idle
	curJobStartTime int

	// Time since last heartbeat received
	lastHeartbeat int
}

type Job struct {
	jobId string
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

func (c *Coordinator) RegisterWorkerRPC(workerId string, reply *GenericReply) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	// Add worker to list of workers
	worker := WorkerData{}
	worker.status = "idle"
	worker.curJobStartTime = -1
	worker.lastHeartbeat = time.Now().Unix()
	c.workers[workerId] = worker

	reply.Success = true
	return nil;
}

func (c *Coordinator) HeartbeatRPC(workerID string, reply *GenericReply) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.workers[workerID].lastHeartbeat = time.Now().Unix()

	reply.Success = true
	return nil;
}

// Assign a task to worker if one is available
func (c *Coordinator) Task(workerID string, reply *TaskReply) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// TODO
	return nil;
}

// Checks every second if any workers have crashed
func (c *Coordinator) heartbeatMonitor() {
	for {
		c.mu.Lock()
		defer c.mu.Unlock()

		for workerId, workerData := range c.workers {
			if time.Now().Unix() - workerData.lastHeartbeat > 10 {
				// Worker has crashed
				c.workers[workerId].status = "crashed"
				if workerData.curJobId != "" {
					// TODO: Reassign job
				}
			}
		}

		time.Sleep(1 * time.Second)
	}
	
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

func (c *Coordinator) initMapTasks(files []string, nReduce int) {
	// TODO
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
	go c.heartbeatMonitor()

	c.server()
	return &c
}
