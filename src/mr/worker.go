package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	for {
		task := CallRequestTask()
		switch task.rtype {
		case IDLE:
			time.Sleep(task.idleTime)
		case COMPLETE:
			fmt.Printf("job complete, no task left. pid %d is over", os.Getpid())
			return
		case MAP:
			{
				file, err := os.Open(task.filename)
				if err != nil {
					log.Fatalf("cannot open %v", task.filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", task.filename)
				}
				file.Close()

				kva := mapf(task.filename, string(content))

				intermediateFile, _ := os.Create(fmt.Sprintf("mr-%d-%d", task.key, ihash(task.filename)))
				enc := json.NewEncoder(intermediateFile)
				for _, kv := range kva {
					err := enc.Encode(&kv)
					if err != nil {
						log.Fatal("failed parsing KeyValue into JSON")
					}
				}
			}
		case REDUCE:
			{

			}
		}
	}

}

func CallRequestTask() RequestTaskReply {
	args := RequestTask{}
	reply := RequestTaskReply{}

	call("Coordinator.RequestTask", &args, &reply)
	return reply
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
