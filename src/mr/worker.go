package mr
import "sort"
import "encoding/json"
import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "os"
import "io/ioutil"
//
// Map functions return a slice of KeyValue.
//``
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
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

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	
	var response TaskReply = TaskReply{}
	var request TaskArgs = TaskArgs{Type: INIT}

	for {
		response = TalktoMaster(&request) // get task from coordinator +  update req done or not
		switch response.Type {
			case MAP:
				doMap(mapf, &request, &response)
			case REDUCE:
				doReduce(reducef, &request, &response)
			case WAIT:
				log.Printf("wait for 500 ms")
				time.Sleep(500 * time.Millisecond)
				request.Type = INIT
			case DONE:
				return
			default:
				//panic and show the type of task, though it can only be INIT 
				panic(fmt.Sprintf("unknown task type: %v", response.Type))
		}
	}
}

//get task from coordinator +  update req done or not
func TalktoMaster(request *TaskArgs) TaskReply {
	reply := TaskReply{}
	ok := call("Coordinator.TalktoWorker", request, &reply)
	if !ok {
		log.Fatal("ask for task failed")
	}
	// log.Printf("ask for task: Type: %v , ID: %v", reply.Type, reply.Id)
	return reply
}

func doMap(mapf func(string, string) []KeyValue, request *TaskArgs,response *TaskReply) {
	log.Printf("do map task: %v", response.Id)
	filename := response.Files[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		log.Fatal(err)
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		log.Fatal(err)
	}

	intermediate := mapf(filename, string(content))
	//get a set of KV pairs with k is the word and v is the 1.
	//assign pairs with same keyHash to the same reduce task(may contains multiple key since user defined how many reduce partitions)

	reduceFileList := make(map[int][]KeyValue)
	for _, kv := range intermediate {
		reduceId := ihash(kv.Key) % response.NReduce
		reduceFileList[reduceId] = append(reduceFileList[reduceId], kv)
	}

	finished_files := make([]string, response.NReduce)
	for reduceId, kvList := range reduceFileList {
		oname := fmt.Sprintf("mr-%v-%v", response.Id, reduceId)
		ofile, _ := os.Create(oname)
		defer ofile.Close()
		enc := json.NewEncoder(ofile)
		for _, kv := range kvList {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal(err)
			}
		}
		finished_files[reduceId] = oname
	}

	//mark task done
	request.Id = response.Id
	request.Type = MAP
	request.Files = finished_files
}

func doReduce(reducef func(string, []string) string, request *TaskArgs, response *TaskReply) {
	log.Printf("do reduce task: %v", response.Id)
	intermediate := make([]KeyValue, 0)
	for _, filename := range response.Files {
		file, err := os.Open(filename)
		if err != nil {
			// show error message
			log.Fatalf("cannot open %v, error: %v", filename, err)
		}
		defer file.Close()

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
		
	oname := fmt.Sprintf("mr-out-%v", response.Id)
	ofile, _ := os.Create(oname)
	defer ofile.Close()

	sort.Sort(ByKey(intermediate))
	
	i := 0
	// iterate through the sorted array, load all the values for the same key into an array value, and call reducef for the value array
	// after that move the i pointer to the j pointer so that we can start with the next key in the file
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	request.Id = response.Id
	request.Type = REDUCE
	request.Files = []string{oname}
}

//
// example function to show how to make an RPC call to the coordinator.
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
