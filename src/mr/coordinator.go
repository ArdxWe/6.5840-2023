package mr

import (
	"errors"
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
	InputFiles  []string
	MapJobCount int64

	ReduceJobCount int64

	AssignMapJob sync.Map
	FinishMapJob sync.Map

	AssignReduceJob sync.Map
	FinishReduceJob sync.Map

	mutex sync.Mutex
}

func lenSyncMap(m *sync.Map) int {
	var i int
	m.Range(func(k, v interface{}) bool {
		i++
		return true
	})
	return i
}

func (c *Coordinator) couldAssignMapJob() bool {
	return lenSyncMap(&c.FinishMapJob)+lenSyncMap(&c.AssignMapJob) != int(c.MapJobCount)
}

func (c *Coordinator) mapJobDone() bool {
	return lenSyncMap(&c.FinishMapJob) == int(c.MapJobCount)
}

func (c *Coordinator) couldAssignReduceJob() bool {
	return lenSyncMap(&c.FinishReduceJob)+lenSyncMap(&c.AssignReduceJob) != int(c.ReduceJobCount) && c.mapJobDone()
}

func (c *Coordinator) reduceJobDone() bool {
	return lenSyncMap(&c.FinishReduceJob) == int(c.ReduceJobCount)
}

func (c *Coordinator) assignMapJob(index int64) {
	// have been assigned
	if _, exist := c.AssignMapJob.Load(index); exist {
		log.Fatalf("map job %+v have been assigned.", index)
	}

	// have been finished
	if _, exist := c.FinishMapJob.Load(index); exist {
		log.Fatalf("map job %+v have been finished.", index)
	}

	c.AssignMapJob.Store(index, true)

	go func() {
		time.Sleep(10 * time.Second)

		c.mutex.Lock()
		defer c.mutex.Unlock()
		if _, exist := c.FinishMapJob.Load(index); !exist {
			c.AssignMapJob.Delete(index)
		}
	}()
}

func (c *Coordinator) assigReduceJob(index int64) {
	// heve been assigned
	if _, exist := c.AssignReduceJob.Load(index); exist {
		log.Fatalf("reduce job %+v have been finished.", index)
	}

	// have been assigned
	if _, exist := c.FinishReduceJob.Load(index); exist {
		log.Fatalf("reduce job %+v have been finished.", index)
	}

	c.AssignReduceJob.Store(index, true)

	go func() {
		time.Sleep(10 * time.Second)

		c.mutex.Lock()
		defer c.mutex.Unlock()

		if _, exist := c.FinishReduceJob.Load(index); !exist {
			c.AssignReduceJob.Delete(index)
		}
	}()
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AssignJob(req *AssignJobRequest, resp *AssignJobResponse) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.Done() {
		resp.Finish = true
		return nil
	}

	if !c.couldAssignMapJob() && !c.couldAssignReduceJob() {
		return errors.New("all could assign has been assigned")
	}

	if c.couldAssignMapJob() {
		// assign map job
		for i := 0; i < int(c.MapJobCount); i++ {
			// have finished
			if _, exist := c.FinishMapJob.Load(int64(i)); exist {
				continue
			}
			// not been assigned
			if _, exist := c.AssignMapJob.Load(int64(i)); !exist {
				resp.JobType = MapJobType
				resp.MapJob.Index = int64(i)
				resp.MapJob.InputFileName = c.InputFiles[i]
				resp.MapJob.ReduceCount = c.ReduceJobCount
				c.assignMapJob(int64(i))
				return nil
			}
		}
	}

	if c.couldAssignReduceJob() {
		// assign reduce job
		for i := 0; i < int(c.ReduceJobCount); i++ {
			// have finished
			if _, exist := c.FinishReduceJob.Load(int64(i)); exist {
				continue
			}
			// not been assigned
			if _, exist := c.AssignReduceJob.Load(int64(i)); !exist {
				resp.JobType = ReduceJobType
				resp.ReduceJob.Index = int64(i)
				resp.ReduceJob.MapCount = c.MapJobCount
				c.assigReduceJob(int64(i))
				return nil
			}
		}
	}

	return nil
}

func (c *Coordinator) FinishJob(req *FinishJobRequest, resp *FinishJobResponse) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if req.JobType == MapJobType {
		if _, exist := c.AssignMapJob.Load(req.Index); exist {
			c.AssignMapJob.Delete(req.Index)
			c.FinishMapJob.Store(req.Index, true)
		} else {
			log.Fatalf("finish map job %+v in assign map", req.Index)
		}
	} else if req.JobType == ReduceJobType {
		if _, exist := c.AssignReduceJob.Load(req.Index); exist {
			c.AssignReduceJob.Delete(req.Index)
			c.FinishReduceJob.Store(req.Index, true)
		} else {
			log.Fatalf("finish reduce job %+v in assign map", req.Index)
		}
	} else {
		log.Fatalf("unknown job type: %+v", req.JobType)
	}

	resp.Finish = c.Done()
	return nil
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.mapJobDone() && c.reduceJobDone()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		InputFiles:  files,
		MapJobCount: int64(len(files)),

		ReduceJobCount: int64(nReduce),
	}

	c.server()
	return &c
}
