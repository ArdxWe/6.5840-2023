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
)

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

// intermediate file format: mr-(map job index)-(reduce job index)
const fileNameformat = "mr-%v-%v"

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		resp, ok := AssignJob()
		if !ok {
			continue
		}
		if resp.Finish {
			return
		}
		if resp.JobType == MapJobType {
			doMap(mapf, resp.MapJob.Index, resp.MapJob.InputFileName, resp.MapJob.ReduceCount)
			if _, ok := FinishJob(resp.MapJob.Index, MapJobType); !ok {
				log.Fatal("assignJob response error")
			}
		} else if resp.JobType == ReduceJobType {
			doReduce(reducef, resp.ReduceJob.Index, resp.ReduceJob.MapCount)
			if _, ok := FinishJob(resp.ReduceJob.Index, ReduceJobType); !ok {
				log.Fatal("finishJob response error")
			}
		}
	}
}

func doMap(mapf func(string, string) []KeyValue, index int64, readFileName string, reduceCount int64) {
	log.Printf("assign map job index: %+v, fileName: %+v, reduce count: %+v", index, readFileName, reduceCount)

	// open reduceCount ouputFile
	var files []*os.File
	for i := 0; i < int(reduceCount); i++ {
		fileName := fmt.Sprintf(fileNameformat, index, i)
		file, err := os.Create(fileName)
		if err != nil {
			log.Fatalf("cannot open %+v", fileName)
		}
		files = append(files, file)
	}

	defer func() {
		for _, file := range files {
			file.Close()
		}
	}()

	// read file content
	file, err := os.Open(readFileName)
	if err != nil {
		log.Fatalf("cannot open %v", readFileName)
	}
	defer file.Close()

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", readFileName)
	}

	// do map
	intermediate := mapf(readFileName, string(content))

	// reduce function need key is sorted
	sort.Sort(ByKey(intermediate))

	for i := 0; i < len(intermediate); i++ {
		fileIndex := ihash(intermediate[i].Key) % int(reduceCount)
		enc := json.NewEncoder(files[fileIndex])
		enc.Encode(KeyValue{
			Key:   intermediate[i].Key,
			Value: intermediate[i].Value,
		})
	}
}

func doReduce(reducef func(string, []string) string, index int64, mapCount int64) {
	log.Printf("assign reduce job index: %+v, mapCount: %+v", index, mapCount)

	reduceOuputFileName := fmt.Sprintf("mr-out-%d", index)
	reduceOuputFile, err := os.Create(reduceOuputFileName)
	if err != nil {
		log.Fatalf("cannot create %+v", reduceOuputFileName)
	}
	defer reduceOuputFile.Close()

	// get all kv data
	var kvs []KeyValue
	for i := 0; i < int(mapCount); i++ {
		fileName := fmt.Sprintf(fileNameformat, i, index)
		kvs = append(kvs, readIntermediatefile(fileName)...)
	}

	// different map file may have same key
	sort.Sort(ByKey(kvs))

	x := 0
	for x < len(kvs) {
		j := x + 1
		for j < len(kvs) && kvs[j].Key == kvs[x].Key {
			j++
		}
		values := []string{}

		// same key get all values
		for k := x; k < j; k++ {
			values = append(values, kvs[k].Value)
		}

		// reduce function, write to file
		output := reducef(kvs[x].Key, values)
		fmt.Fprintf(reduceOuputFile, "%v %v\n", kvs[x].Key, output)
		x = j
	}
}

func readIntermediatefile(fileName string) []KeyValue {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %+v, err: %+v", fileName, err)
	}
	defer file.Close()

	dec := json.NewDecoder(file)
	var res []KeyValue
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		res = append(res, kv)
	}
	return res
}

func AssignJob() (*AssignJobResponse, bool) {
	req := AssignJobRequest{}
	resp := AssignJobResponse{}
	ok := call("Coordinator.AssignJob", &req, &resp)
	return &resp, ok
}

func FinishJob(index int64, jobType JobType) (*FinishJobResponse, bool) {
	req := FinishJobRequest{
		Index:   index,
		JobType: jobType,
	}
	resp := FinishJobResponse{}
	ok := call("Coordinator.FinishJob", &req, &resp)
	return &resp, ok
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
