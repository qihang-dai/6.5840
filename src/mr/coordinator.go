package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

// only in coordinator for assign and mark done status

type Task struct{
	Type TaskType
	Id int
	Files []string
	Start_time time.Time //default value is long time ago 
	Done bool
}

type Coordinator struct {
	// Your definitions here.
	mu sync.RWMutex
	reduceLeft int // the number of reduce tasks is defined by the caller
	mapleft int // the number of map tasks is the number of files
	mapTasks []Task
	reduceTasks []Task
}

// Your code here -- RPC handlers for the worker to call.
func Communicate(request *TaskArgs, response *TaskReply) {
	c.RWMutex.Lock()
	defer c.RWMutex.Unlock()

	//when the worker finished the task, it will send a request with Map or Reduce statusto the coordinator.
	//we mark the task in the task queue as done if the task is not done yet (in case of duplicate request)
	switch request.Type {
		case MAP:
			if !c.mapTasks[request.Id].Done {
				c.mapTasks[request.Id].Done = true

				//when the worker finished the map task, it will send the processed intermediate file name to the coordinator
				//we will assign the intermediate file name to the reduce task
				for _, filename := range request.Files {
					c.reduceTasks[request.Id].Files = append(c.reduceTasks[request.Id].Files, filename)
				}
				c.mapleft--
			}
		case REDUCE:
			if !c.reduceTasks[request.Id].Done {
				c.reduceTasks[request.Id].Done = true
				c.reduceleft--
			}
		case INIT:
			//when the worker request a task, it will send a INIT request to the coordinator
			//we will assign a map task or reduce task to the worker
			if c.nMap > 0 {
				//check if there is (unfinished map task) and (has been work for more than 10 seconds) 
				//new task will also be assinged since new task its always time out since the start time is long time ago
				// if so, assign the task to a new worker
				for i, task := range c.mapTasks {
					if !task.Done && time.Since(task.Start_time) > 10 * time.Second {
						response.Type = MAP
						response.Id = i
						response.Files = task.Files
						response.NReduce = len(c.reduceTasks) //assign total number of reduce task to the worker for hash function
						task.Start_time = time.Now()
						return // assign task and return, no need to check other tasks since we only assign one task at a time
					}
					//if there is no unfinished map task, ask the worker to wait till all maps are done and then start reduce
					response.Type = WAIT
				}
			} else if c.nReduce > 0 { //if all map tasks are done, assign reduce task
				for i, task := range c.reduceTasks {
					if !task.Done && time.Since(task.Start_time) > 10 * time.Second {
						response.Type = REDUCE
						response.Id = i
						response.Files = task.Files
						response.NReduce = len(c.reduceTasks)
						task.Start_time = time.Now()
						return
					}
					response.Type = WAIT //if there is no unfinished reduce task, ask the worker to wait till all reduces are done
				}
			}else{
				response.Type = DONE //if all map and reduce tasks are done, tell the worker to exit
			}
	}

	// Now neith


//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reduceLeft == 0 && c.mapLeft == 0 {
		ret = true
		log.Printf("All tasks are done")
	}
	log.Printf("reduceLeft: %d, mapLeft: %d", c.reduceLeft, c.mapLeft)

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
	c := Coordinator{
		reduceLeft: nReduce,
		mapLeft: len(files),
		mapTasks: make([]Task, len(files)),
		reduceTasks: make([]Task, nReduce),
	}

	c.server()
	return &c
}
