package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
	"io/ioutil"
)

// log show line number and file name
func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	log.SetOutput(ioutil.Discard)
}

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
	mu sync.Mutex
	reduceleft int // the number of reduce tasks is defined by the caller
	mapleft int // the number of map tasks is the number of files
	mapTasks []Task
	reduceTasks []Task
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) TalktoWorker(request *TaskArgs, response *TaskReply) error{
	c.mu.Lock()
	defer c.mu.Unlock()
	// log.Printf("request: Type: %v , ID: %v", request.Type, request.Id)

	//when the worker finished the task, it will send a request with Map or Reduce statusto the coordinator.
	//we mark the task in the task queue as done if the task is not done yet (in case of duplicate request)

	//check if the request is a done request
	switch request.Type {
		case MAP:
			if !c.mapTasks[request.Id].Done {
				c.mapTasks[request.Id].Done = true
				log.Printf("map task %v done", request.Id)

				//when the worker finished the map task, it will send the processed intermediate file name to the coordinator
				//we will assign the intermediate file name to the reduce tas
				for id, file := range request.Files {
					if len(file) > 0 {
						c.reduceTasks[id].Files = append(c.reduceTasks[id].Files, file)
					}
				}

				c.mapleft--
			}
		case REDUCE:
			if !c.reduceTasks[request.Id].Done {
				log.Printf("reduce task %v done", request.Id)
				c.reduceTasks[request.Id].Done = true
				c.reduceleft--
			}
	}
	
	//assign task to the worker everytime it request a task

	//when the worker request a task, it will send a INIT request to the coordinator
	//we will assign a map task or reduce task to the worker
	now := time.Now()
	timeoutAgo := now.Add(-10 * time.Second)
	if c.mapleft> 0 {
		//check if there is (unfinished map task) and (has been work for more than 10 seconds) 
		//new task will also be assinged since new task its always time out since the start time is long time ago
		// if so, assign the task to a new worker
		for idx := range c.mapTasks {
			t := &c.mapTasks[idx]
			if !t.Done && t.Start_time.Before(timeoutAgo) {
				response.Type = MAP
				response.Id = t.Id
				response.Files = t.Files
				response.NReduce = len(c.reduceTasks)
				t.Start_time = now 
				return nil
			}
		}
		//if all tasks have been started and not timed out || done, ask the worker to wait
		response.Type = WAIT
	} else if c.reduceleft> 0 { //if all map tasks are done, assign reduce task
		for idx := range c.reduceTasks {
			t := &c.reduceTasks[idx]
			if !t.Done && t.Start_time.Before(timeoutAgo) {
				response.Type = REDUCE
				response.Id = t.Id
				response.Files = t.Files
				t.Start_time = now 
				return nil
			}
		}
		response.Type = WAIT //if there is no unfinished reduce task, ask the worker to wait till all reduces are done
	}else{
		response.Type = DONE //if all map and reduce tasks are done, tell the worker to exit
		log.Printf("your job is done")
	}
	//print the assigned task
	log.Printf("response: Type: %v , ID: %v", response.Type, response.Id)
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
	if c.reduceleft == 0 && c.mapleft == 0 {
		ret = true
		log.Printf("All tasks are done")
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		reduceleft: nReduce,
		mapleft: len(files),
		mapTasks: make([]Task, len(files)),
		reduceTasks: make([]Task, nReduce),
	}

	for i, file := range files {
		c.mapTasks[i] = Task{
			Type: MAP,
			Id: i,
			Files: []string{file},
			Done: false,
		}
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{
			Type: REDUCE,
			Id: i,
			Files: []string{},
			Done: false,
		}
	}
	
	//print coordinator info
	log.Print("Coordinator info:")
	log.Printf("Number of map tasks: %v", c.mapleft)
	log.Printf("Number of reduce tasks: %v", c.reduceleft)
	log.Printf("len of mapTasks: %v", len(c.mapTasks))
	log.Printf("len of reduceTasks: %v", len(c.reduceTasks))

	c.server()
	return &c
}
