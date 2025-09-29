package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	nReduce int

	workers    map[int64]WorkerData
	mapJobs    []Job
	reduceJobs []Job
	mu         sync.Mutex

	// "map", "reduce", "done"
	phase string
}

type WorkerData struct {
	// "idle", "busy", "crashed"
	// status might not be needed
	status string

	// "map", "reduce"
	curJobType string

	// -1 if no job
	curJobId int64

	// Time when the worker started its current task, -1 if idle
	curJobStartTime int64

	// Time since last heartbeat received
	lastHeartbeat int64
}

type Job struct {
	// jobId should be index in mapJobs or reduceJobs
	// If this is no longer the case then add it here
	//jobId int

	file string

	// Time when worker started this job
	// If multiple workers on the same job, time should be the mot recent start time
	jobStartTime int64

	// "incomplete", "in progress", "complete"
	status string
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RegisterWorkerRPC(args *WorkerArgs, reply *GenericReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Add worker to list of workers
	worker := WorkerData{}
	worker.curJobId = args.WorkerId
	worker.curJobType = args.JobType
	worker.status = "idle"
	worker.curJobStartTime = -1
	worker.lastHeartbeat = time.Now().Unix()
	c.workers[args.WorkerId] = worker

	reply.Success = true
	return nil
}

func (c *Coordinator) HeartbeatRPC(args *WorkerArgs, reply *GenericReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	newWorkerData := c.workers[args.WorkerId]
	newWorkerData.lastHeartbeat = time.Now().Unix()
	c.workers[args.WorkerId] = newWorkerData

	reply.Success = true
	return nil
}

// Assign a task to worker if one is available
// Iterate through the jobs array and assigns the first one that is either:
// - status is "incomplete"
// - status is "in progress" and jobStartTime is more than 10 seconds ago

// side note: if we stick to this assigning logic, idk what use the worker status would be used for.
// not even sure if what the heartbeat monitor is used for either, but it's there anyways
func (c *Coordinator) Task(args *WorkerArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.phase == "map" {
		for id, jobData := range c.mapJobs {
			if jobData.status == "incomplete" || (jobData.status == "in progress" && time.Now().Unix()-jobData.jobStartTime > 10) {
				reply.JobId = id
				reply.File = jobData.file
				reply.NReduce = c.nReduce

				newJobData := jobData
				newJobData.status = "in progress"
				newJobData.jobStartTime = time.Now().Unix()
				c.mapJobs[id] = newJobData

				return nil
			}
		}

		// No available tasks
		reply.JobId = -1

	} else {
		// TODO assign reduce task
	}

	return nil
}

func (c *Coordinator) WorkerCompletion(args *CompletionArgs, reply *GenericReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.JobType {
	case "map":
		c.mapJobs[args.JobId].status = "complete"
	case "reduce":
		c.reduceJobs[args.JobId].status = "complete"
	}
	reply.Success = true
	return nil
}

// Checks every second if any workers have crashed
func (c *Coordinator) heartbeatMonitor() {
	for {
		c.mu.Lock()

		for workerId, workerData := range c.workers {
			newWorkerData := workerData
			if time.Now().Unix()-workerData.lastHeartbeat > 10 {
				// Worker has crashed
				newWorkerData.status = "crashed"

			}
			c.workers[workerId] = newWorkerData
		}

		c.mu.Unlock()
		time.Sleep(1 * time.Second)
	}
}

// Checks every second if all map tasks are complete
func (c *Coordinator) mapTasksMonitor() {
	for {
		c.mu.Lock()

		done := true
		for _, jobData := range c.mapJobs {
			if jobData.status != "complete" {
				done = false
				break
			}
		}

		if done {
			c.phase = "done" // TODO: switch this to "reduce" when reduce is implemented (it is "done" right now so the code exits)
			c.mu.Unlock()
			return
		}

		c.mu.Unlock()
		time.Sleep(1 * time.Second)
	}
}

// start a thread that listens for RPCs from worker.go
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

func (c *Coordinator) initMapTasks(files []string) {
	// Add each file as a job inside Coordinator
	for _, file := range files {
		job := Job{}
		job.file = file
		job.status = "incomplete"
		c.mapJobs = append(c.mapJobs, job)
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.

	return c.phase == "done"
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nReduce = nReduce
	c.phase = "map"

	// Initialize the workers map
	c.workers = make(map[int64]WorkerData)
	c.initMapTasks(files)

	// Monitors
	go c.heartbeatMonitor()
	go c.mapTasksMonitor()

	c.server()
	return &c
}
