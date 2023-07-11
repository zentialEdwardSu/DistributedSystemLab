package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func ReportFailed(ts *TaskState) {
	ts.State = _failed
	callTaskReport(ts)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	fmt.Println("[Worker] online")
	// Your worker implementation here.
	for {
		task := callTask()
		isfailed := false

		task.Log("[Worker]")
		switch task.State {
		case _map:
			isfailed = workerMap(mapf, task)
		case _reduce:
			isfailed = workerReduce(reducef, task)
		case _wait:
			// wait for 5 seconds to requeset again
			time.Sleep(time.Duration(time.Second * 5))
		case _end:
			fmt.Println("[Worker] All job done. Worker Goodbye....")
			return
		default:
			log.Fatalln("[Worker] Invalid TaskState!!")
		}

		// acknowledge master
		if task.State == _map || task.State == _reduce { // only report when in _map or reduce
			if isfailed {
				ReportFailed(task) // failed change taskState to _failed in order to mark the failure
			} else {
				callTaskReport(task) // success report task done
			}
		}
		isfailed = false // reset status
	}
}

func callTask() *TaskState {
	args := ExampleArgs{}
	reply := TaskState{}
	err := call("Coordinator.HandleCallTask", &args, &reply)
	//reply.Log("[Worker]")

	if !err {
		fmt.Println("[Worker] Something wrong happened when request for task")
		reply.State = _wait // wait for new request while error ocoured
	}
	return &reply

}

func callTaskReport(task *TaskState) {
	reply := ExampleReply{}
	err := call("Coordinator.HandleTaskReport", &task, &reply)

	if !err {
		fmt.Println("[Worker] Something wrong when pass TaskState")
	}
}

func workerMap(mapf func(string, string) []KeyValue, taskInfo *TaskState) bool {
	fmt.Printf("[Worker] Got assigned map task on %vth file %s\n", taskInfo.MapIndex, taskInfo.FileName)

	// read in target files as a key-value array
	intermediate := []KeyValue{}
	file, err := os.Open(taskInfo.FileName)
	if err != nil {
		log.Fatalf("[Worker] cannot open %v", taskInfo.FileName)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("[Worker] cannot read %v", taskInfo.FileName)
		return true
	}
	file.Close()
	kva := mapf(taskInfo.FileName, string(content)) //mapf convert file content to kv pair
	intermediate = append(intermediate, kva...)

	// prepare output files and encoders
	currentDir, err := os.Getwd() // get work dir
	if err != nil {
		fmt.Println("Failed to get current directory:", err)
		return true
	}
	nReduce := taskInfo.R
	outprefix := "mr-"
	outprefix += strconv.Itoa(taskInfo.MapIndex)
	outprefix += "-" // outputprefix "mr-$MapIndex-"
	outFiles := make([]*os.File, nReduce)
	fileEncs := make([]*json.Encoder, nReduce)

	// create tempfile and json encoder
	for outindex := 0; outindex < nReduce; outindex++ {
		outFiles[outindex], _ = os.CreateTemp(currentDir, "mr-tmp-*")
		fileEncs[outindex] = json.NewEncoder(outFiles[outindex])
	}

	// distribute keys into tmpfiles mr-tmp-$outindex(by hash)
	for _, kv := range intermediate {
		outindex := ihash(kv.Key) % nReduce // choose file piece by hash
		file = outFiles[outindex]
		enc := fileEncs[outindex]
		err := enc.Encode(&kv) // write temp
		if err != nil {
			fmt.Printf("[Worker] File %v Key %v Value %v Error: %v\n", taskInfo.FileName, kv.Key, kv.Value, err)
			//panic("Json encode failed")
			return true
		}
	}

	// rename to mr-$MapIndex-$ReduceIndex
	for outindex, file := range outFiles {
		outname := outprefix + strconv.Itoa(outindex)
		oldpath := filepath.Join(file.Name())
		os.Rename(oldpath, outname)
		file.Close()
	}

	return false // no error happened
}

func workerReduce(reducef func(string, []string) string, taskInfo *TaskState) bool {
	fmt.Printf("[Worker] Got assigned reduce task on part %v\n", taskInfo.ReduceIndex)
	outname := "mr-out-" + strconv.Itoa(taskInfo.ReduceIndex)
	currentDir, err := os.Getwd()
	if err != nil {
		fmt.Println("Failed to get current directory:", err)
		return true
	}
	// read from output files from map tasks

	innameprefix := "mr-"
	innamesuffix := "-" + strconv.Itoa(taskInfo.ReduceIndex)

	// read in all files as a kv array
	intermediate := []KeyValue{}
	for index := 0; index < taskInfo.M; index++ {
		inname := innameprefix + strconv.Itoa(index) + innamesuffix
		file, err := os.Open(inname)
		if err != nil {
			fmt.Printf("[Worker] Open intermediate file %v failed: %v\n", inname, err)
			//panic("Open file error")
			return true
		}

		dec := json.NewDecoder(file)
		for {
			kv := KeyValue{}
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	// sort
	sort.Sort(ByKey(intermediate))

	ofile, err := os.CreateTemp(currentDir, "mr-*")
	if err != nil {
		fmt.Printf("[Worker] Create output file %v failed: %v\n", outname, err)
		//panic("Create file error")
		return true
	}

	// compose to A:[1 1 1 1]
	composedKV := make(map[string][]string)
	for _, pair := range intermediate {
		composedKV[pair.Key] = append(composedKV[pair.Key], pair.Value)
	}

	// sort
	sortedKeys := make([]string, 0, len(composedKV))
	for key := range composedKV {
		sortedKeys = append(sortedKeys, key)
	}
	sort.Strings(sortedKeys)

	// save to file
	for _, key := range sortedKeys {
		output := reducef(key, composedKV[key])
		fmt.Fprintf(ofile, "%v %v\n", key, output)
	}

	os.Rename(filepath.Join(ofile.Name()), outname)
	ofile.Close()

	return false
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

	fmt.Printf("[Worker] Error when call RPC: %v\n", err)
	return false
}
