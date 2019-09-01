package mapreduce

import (
	"container/list"
	"fmt"
	"sync"
)

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

type Process struct {
	finished int
	mu       sync.Mutex
}

func (p *Process) Incr() {
	p.mu.Lock()
	p.finished++
	p.mu.Unlock()
}

func schdual(mr *MapReduce, produceCount int, consumeCount int, jobType JobType) {

	// fail := -1
	// for i := 0; i < produceCount || fail != -1; i++ {
	// 	arg := new(DoJobArgs)
	// 	arg.File = mr.file
	// 	if i >= produceCount {
	// 		arg.JobNumber = fail
	// 		fail = -1
	// 	} else {
	// 		arg.JobNumber = i
	// 	}

	// 	arg.Operation = jobType
	// 	arg.NumOtherPhase = consumeCount
	// 	work := <-mr.registerChannel
	// 	go func(w string, i int) {
	// 		var res DoJobReply
	// 		err := call(w, "Worker.DoJob", &arg, &res)
	// 		if !err {
	// 			fail = i
	// 		}
	// 		if res.OK {
	// 			mr.registerChannel <- w
	// 		}
	// 	}(work, i)
	// }

	process := Process{finished: 0}
	jn := make(chan int, produceCount)
	for i := 0; i < produceCount; i++ {
		jn <- i
	}
	for process.finished < produceCount {
		jobNumber, ok := <-jn
		if !ok {
			break
		}
		arg := new(DoJobArgs)
		arg.File = mr.file
		arg.JobNumber = jobNumber
		arg.Operation = jobType
		arg.NumOtherPhase = consumeCount
		work := <-mr.registerChannel
		go func(w string, i int) {
			var res DoJobReply
			err := call(w, "Worker.DoJob", &arg, &res)
			if !err {
				jn <- i
			}
			if res.OK {
				process.Incr()
				if process.finished >= produceCount {
					close(jn)
				}
				mr.registerChannel <- w
			}
		}(work, jobNumber)
	}
}
func (mr *MapReduce) RunMaster() *list.List {

	schdual(mr, mr.nMap, mr.nReduce, Map)
	schdual(mr, mr.nReduce, mr.nMap, Reduce)
	return mr.KillWorkers()
}
