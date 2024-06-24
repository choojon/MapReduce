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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

// mr-main/mrworker.go call	s this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// use mapf's output in reducef
	// Write mapf's output into intermediate files - inputs (filename{ignore}, contents of file{open prior})
	//reduce function input - (key, filtered all contents-> number of occurences (asd:1,asd:1))
	// Reduce function reads all intermediates (open all at same time) and basically gathers up the key
	//
	// Call reduce for each unique key, write it to mr-out-X
	//args := TaskArgs{}
	//reply := TaskReply{}
	for {
		args := TaskArgs{}
		reply := TaskReply{}
		//reply.tasktype = " "
		call("Coordinator.Coordinate", &args, &reply)
		if reply.Tasktype == "shutdown" {
			fmt.Println("Exiting worker")
			break
		}
		if reply.Tasktype == "mtask" {
			//fmt.Printf("map assigned")
			filename := reply.Filename
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			valueshashed := make([][]KeyValue, reply.Nreduce) // Make an array of arrays to store hashed values
			kva := mapf(filename, string(content))

			for _, key := range kva {

				hashvalue := ihash(key.Key) % reply.Nreduce
				valueshashed[hashvalue] = append(valueshashed[hashvalue], key)
			}
			/*valuemap := map[int]*json.Encoder{}
			for i := 0; i < reply.Nreduce; i++ { //Make intermediate files
				//var err error
				newfile := fmt.Sprintf("mr-%v-%v", reply.Partitionnum, i)
				file, _ := os.Create(newfile)
				valuemap[i] = json.NewEncoder(file)
			}
			for _, kvpair := range kva {
				hashv := ihash(kvpair.Key) % reply.Nreduce
				valuemap[hashv].Encode(&kvpair)
			}*/
			//Now we have to write each array in the array into it's intermediate file
			for i := 0; i < reply.Nreduce; i++ { //Make intermediate files
				//var err error
				newfile := fmt.Sprintf("mr-%v-%v", reply.Partitionnum, i)
				file, _ := os.Create(newfile)
				enc := json.NewEncoder(file)
				err = enc.Encode(&valueshashed[i])

			}
			fmt.Printf("Map %v done\n", reply.Partitionnum)
			argd := TaskConfirmedArgs{"mtask", reply.Partitionnum}
			replyd := TaskConfirmedReply{}
			call("Coordinator.Updatetask", &argd, &replyd)
		} else if reply.Tasktype == "rtask" {
			//fmt.Printf("reduced assigned")
			filenames := []string{}
			kvpairs := []KeyValue{}
			//fmt.Println(reply.Partitionnum)
			partnum := reply.Partitionnum
			for i := 0; i < reply.Nreduce; i++ {
				name := fmt.Sprintf("mr-%v-%v", i, partnum)
				filenames = append(filenames, name)
			}
			//fmt.Println(filenames)
			//fmt.Println(partnum)
			for _, filename := range filenames { //Concoctonate all files into one array
				file, _ := os.Open(filename)
				dec := json.NewDecoder(file)
				for {
					var kv []KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kvpairs = append(kvpairs, kv...)
				}
				file.Close()
			}
			sort.Sort(ByKey(kvpairs))
			oname := fmt.Sprintf("mr-out-%v", reply.Partitionnum)
			ofile, _ := os.Create(oname)
			i := 0
			for i < len(kvpairs) {
				j := i + 1
				for j < len(kvpairs) && kvpairs[j].Key == kvpairs[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kvpairs[k].Value)
				}
				output := reducef(kvpairs[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kvpairs[i].Key, output)

				i = j
			}

			fmt.Printf("Reduce %v done\n", reply.Partitionnum)
			argd := TaskConfirmedArgs{"rtask", reply.Partitionnum}
			replyd := TaskConfirmedReply{}
			call("Coordinator.Updatetask", &argd, &replyd)
		} else {
			//fmt.Printf("sleep assigned")
			time.Sleep(2 * time.Second)
			//no task to be done (sleep 10s)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

}

// example function to show how to make an RPC call to the coordinator.
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
// DO NOT MODIFY
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

	fmt.Println("Unable to Call", rpcname, "- Got error:", err)
	return false
}
