package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	Maptasks    chan *TaskStatus // Load up unfinished map tasks
	Reducetasks chan *TaskStatus // Load up unfinished reduce tasks
	MapArray    []TaskStatus
	ReduceArray []TaskStatus
	Maptotal    int
	Reducetotal int
	Mapdone     int
	Reducedone  int
	NReduce     int
	lock        *sync.Mutex
	Mapfin      bool
	Reducefin   bool
}
type TaskStatus struct {
	PartNum     int
	filename    string
	TimeStarted time.Time
	Status      string
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Coordinate(args *TaskArgs, reply *TaskReply) error {
	//reply.Tasktype = "mtask"
	//reply.Partitionnum = 1
	//fmt.Printf("task recevied")
	c.lock.Lock()
	if c.Mapfin == false && len(c.Maptasks) != 0 {
		//Assign map to worker
		var task *TaskStatus
		for {
			task = <-c.Maptasks
			if task.Status == "completed" {
			} else {
				break
			}
		}
		//task := <-c.Maptasks
		reply.Filename = task.filename
		reply.Partitionnum = task.PartNum
		reply.Tasktype = "mtask"
		reply.Nreduce = c.NReduce
		c.MapArray[task.PartNum].TimeStarted = time.Now()
		c.MapArray[task.PartNum].Status = "progressing"
		//fmt.Println(task.TimeStarted)
		//fmt.Println(len(c.Maptasks))
		c.lock.Unlock()
		return nil
	} else if c.Reducefin == false && len(c.Reducetasks) != 0 && c.Mapfin == true {
		//Assign reduce to worker
		var rtask *TaskStatus
		for {
			rtask = <-c.Reducetasks
			if rtask.Status == "completed" {
			} else {
				break
			}
		}
		reply.Filename = "non"
		reply.Nreduce = c.Maptotal
		reply.Partitionnum = rtask.PartNum
		reply.Tasktype = "rtask"
		c.ReduceArray[rtask.PartNum].TimeStarted = time.Now()
		c.ReduceArray[rtask.PartNum].Status = "progressing"
		//fmt.Println(len(c.Reducetasks))
		//fmt.Println("reduce assigned")
		//fmt.Println(reply.Partitionnum)
		//fmt.Println(rtask.PartNum)
		c.lock.Unlock()
		return nil
	} else if c.Mapfin == true && c.Reducefin == true {
		// Fully complete, send a shutdown order
		fmt.Println("Shutting down coordinator")
		reply.Tasktype = "shutdown"
		c.lock.Unlock()
		return nil
	} else {
		// Either map hasn't finished or reduce hasn't finished with no tasks in queue
		// We're waiting, tell worker to standby
		reply.Tasktype = "wait"
		c.lock.Unlock()
		return nil
	}
	//return nil
}

func (c *Coordinator) Updatetask(argsd *TaskConfirmedArgs, replyd *TaskConfirmedReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if argsd.Tasktype == "mtask" {
		if c.MapArray[argsd.Partitionnum].Status == "completed" {
			//fmt.Println("Duplicate done")
			return nil
		} else { //Received an update for a task already completed
			if c.MapArray[argsd.Partitionnum].Status == "completed" {
				//fmt.Println("Duplicate done")
				return nil
			} else {
				c.MapArray[argsd.Partitionnum].Status = "completed"
				//fmt.Println("Map done +1")
				//fmt.Println(c.MapArray)
				c.Mapdone = c.Mapdone + 1
			}
		}
	} else if argsd.Tasktype == "rtask" {
		if c.ReduceArray[argsd.Partitionnum].Status == "completed" {
			return nil
		} else { //Received an update for a task already completed
			c.ReduceArray[argsd.Partitionnum].Status = "completed"
			//fmt.Println("Reduce done +1")
			//fmt.Println(c.ReduceArray)
			c.Reducedone = c.Reducedone + 1
		}
	}
	if c.Mapfin != true { // We've completed all map tasks
		if c.Mapdone == c.Maptotal {
			c.Mapfin = true
			fmt.Println("Maps all done")
		}
	} else if c.Reducefin != true { // We've completed all reduce tasks
		if c.Reducedone == c.Reducetotal {
			c.Reducefin = true
			fmt.Println("Reduces all done")
		}
	}
	return nil
}
func (c *Coordinator) reschedule() error {
	for {
		//fmt.Println("Testing reschedule")
		c.lock.Lock()
		if c.Maptotal != c.Mapdone { // There are unfinished map tasks
			for index, task := range c.MapArray {
				if task.Status == "progressing" { //Check if task is in progress
					TimeNow := time.Now()
					StartTime := task.TimeStarted
					timediff := TimeNow.Sub(StartTime).Seconds()
					if timediff > 10 {
						// We assume it timed out after taking >10 seconds
						//fmt.Println(TimeNow)
						//fmt.Println(StartTime)
						//fmt.Println(timediff)
						c.Maptasks <- &c.MapArray[index]       //Put the task back in queue
						c.MapArray[index].Status = "unstarted" //Set status back to "Unstarted"
						c.MapArray[index].TimeStarted = time.Time{}
						fmt.Printf("Timeout happened task %v", task.PartNum)
					}

				}
			}
		} else if c.Reducetotal != c.Reducedone { // There are unfinished reduce tasks
			for index, task := range c.ReduceArray {
				if task.Status == "progressing" { //Check if task is in progress
					TimeNow := time.Now()
					StartTime := task.TimeStarted
					timediff := TimeNow.Sub(StartTime).Seconds()
					if timediff > 10 {
						// We assume it timed out after taking >10 seconds
						//fmt.Println(TimeNow)
						//fmt.Println(StartTime)
						//fmt.Println(timediff)
						c.Reducetasks <- &c.ReduceArray[index]    //Put the task back in queue
						c.ReduceArray[index].Status = "unstarted" //Set status back to "Unstarted"
						c.ReduceArray[index].TimeStarted = time.Time{}
						fmt.Printf("Timeout happened task %v", task.PartNum)
					}

				}
			}
		} else if c.Mapfin == true && c.Reducefin == true { // Both finished, close reschedule
			fmt.Println("sched done")
			c.lock.Unlock()
			return nil
		}
		//fmt.Println("Resched exited")
		c.lock.Unlock()
		time.Sleep(2 * time.Second)
	}
}

// an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// mr-main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	for {
		c.lock.Lock()
		if c.Mapfin == true && c.Reducefin == true { //maps and reduces are done
			c.lock.Unlock()
			ret = true
			break
		} else {
			c.lock.Unlock()
			time.Sleep(11 * time.Second)
		}
	}
	return ret
}

// create a Coordinator.
// mr-main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Want to make a array that stores all the map tasks
	// And a queue-like slice that stores all queued jobs to be done
	// To do this ensure to pass a pointer through so that the initial array is edited
	// If passing the literal TaskStatus it won't update the original array
	//fmt.Printf("Test")
	c := Coordinator{}
	//var mapa [5]TaskStatus
	c.Maptasks = make(chan *TaskStatus, 30)
	for i := 0; i < len(files); i++ { // SET UP MAP TASKS
		c.MapArray = append(c.MapArray, TaskStatus{i, files[i], time.Time{}, "unstarted"})
		c.Maptasks <- &c.MapArray[i]
	}
	c.Maptotal = len(files)
	c.Mapdone = 0
	//fmt.Printf("Reached map end")
	// Plan is to compare mapremaining and mapdone in scheduler, if timeout>10 then reschedule
	c.Reducetasks = make(chan *TaskStatus, 30)
	for j := 0; j < nReduce; j++ {
		c.ReduceArray = append(c.ReduceArray, TaskStatus{j, "non", time.Time{}, "unstarted"})
		c.Reducetasks <- &c.ReduceArray[j]
	}
	//fmt.Printf("Reached reduce end")
	c.Reducetotal = nReduce
	c.Reducedone = 0
	newlock := sync.Mutex{}
	c.lock = &newlock
	c.Mapfin = false
	c.Reducefin = false
	c.NReduce = nReduce
	// DEBUGGING/TEST LINES
	//c.MapArray = append(c.MapArray, TaskStatus{})
	//fmt.Println(c.MapArray)
	//fmt.Println("dd")
	//c.MapArray[0].PartNum = 2
	//fmt.Println(c.MapArray[0].PartNum)
	//c.Maptasks <- &c.MapArray[0]
	//fmt.Println("des")
	//fmt.Println(c.MapArray[0].PartNum)
	//d := <-c.Maptasks
	//d.PartNum += 1
	//fmt.Println(c.MapArray[0].PartNum)
	//fmt.Printf("%v", c.MapArray[0].PartNum)
	//fmt.Printf("%v", d.PartNum)

	// Your code here.
	go c.reschedule()
	go c.Done()
	c.server()
	//fmt.Printf("SEtup done")
	return &c
}

// start a thread that listens for RPCs from worker.go
// DO NOT MODIFY
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
