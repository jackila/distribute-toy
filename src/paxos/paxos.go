package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const INT_MAX = int(^uint(0) >> 1)
const INT_MIN = ^INT_MAX

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	//be better with list then it can control the list sie
	prepareStatus *LinkedList
	doneMins      []int
	localDoneMin  int
}

type AcceptorState struct {
	NP   int
	NA   int
	VA   interface{}
	Done bool
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()
	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.

	//args := RequestArgs{1, nil, 3, "12"}
	go func(v interface{}) {
		//choose unique n

		n := (int(time.Now().Unix()) << 5) | px.me
		aok := false
		//send prepare to all
		value, pok := prepare(n, px, v, seq)
		//send accept to all
		if pok {
			aok = accept(n, px, value, seq)
			//log.Printf("the seq is %d,the number is %d and the aok is %t", seq, n, aok)
		}
		if aok {
			//send decide to all
			decided(seq, px, value)
		}
	}(v)

}

//prepare method
func prepare(n int, px *Paxos, v interface{}, seq int) (interface{}, bool) {
	pok := false
	value := v
	args := RequestArgs{n, nil, seq, px.localDoneMin, px.me}
	successReply := []ResponseReply{}
	for index, s := range px.peers {
		reply := ResponseReply{}
		if px.me != index {
			call(s, "Paxos.PrepareHandler", args, &reply)
		} else {
			px.PrepareHandler(args, &reply)
		}
		if reply.Success {
			successReply = append(successReply, reply)
		}
	}
	if len(successReply) > len(px.peers)/2 {
		t := -1
		pok = true
		for _, reply := range successReply {
			if reply.N_A > t && reply.V_A != nil {
				value = reply.V_A
				t = reply.N_A
			}
		}

	}
	return value, pok
}

//accept method
func accept(n int, px *Paxos, v interface{}, seq int) bool {

	args := RequestArgs{n, v, seq, px.localDoneMin, px.me}
	successReply := []ResponseReply{}
	for index, s := range px.peers {
		reply := ResponseReply{}
		if px.me != index {
			call(s, "Paxos.AcceptHandler", args, &reply)
		} else {
			px.AcceptHandler(args, &reply)
		}
		if reply.AcceptSuccess {
			successReply = append(successReply, reply)
		}
	}

	if len(successReply) > len(px.peers)/2 {
		return true
	}
	return false
}

// decide method
func decided(seq int, px *Paxos, v interface{}) {

	//peer no langer to need status so will check min
	successReply := []ResponseReply{}
	for index, s := range px.peers {
		args := RequestArgs{0, v, seq, px.localDoneMin, px.me}
		args.Seq = seq
		reply := ResponseReply{}
		if px.me != index {
			//log.Printf("now the args is %+v,and the server is %s", args, s)
			call(s, "Paxos.DecidedHandler", args, &reply)
		} else {
			//log.Printf("local now the args is %+v,and the server is local", args)
			px.DecidedHandler(args, &reply)
		}
		if reply.Success {

			successReply = append(successReply, reply)
		}
	}

}

// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	px.localDoneMin = seq
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	max := -1
	head := px.prepareStatus.Head
	for head.Next != nil {
		state := head.Next
		if max < state.Seq {
			max = state.Seq
		}
		head = head.Next
	}
	return max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	min := px.localDoneMin
	for _, v := range px.doneMins {
		if min > v {
			min = v
		}
	}
	//清理数据,或者放到decide中清理,但是总是会调用这个min的
	head := px.prepareStatus.Head
	for head.Next != nil {
		seq := head.Seq
		head = head.Next
		if seq < min && seq != -1 {
			//log.Printf("now delete element of %d", seq)
			px.prepareStatus.DeleteElem(seq)
		}
	}
	return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	if seq < px.Min() {
		return Forgotten, nil
	}

	node, ok := px.prepareStatus.Find(seq)
	if ok && node.State.Done {
		return Decided, node.State.VA
	}
	return Pending, nil
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

type RequestArgs struct {
	N            int
	V            interface{}
	Seq          int
	MinForgotten int
	Me           int
}

type ResponseReply struct {
	Success       bool
	AcceptSuccess bool
	N             int
	N_A           int
	V_A           interface{}
	MinForgotten  int
}

type RA struct {
	Word string
}

//Prepare function
func (px *Paxos) PrepareHandler(args RequestArgs, reply *ResponseReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	seq := args.Seq
	n := args.N

	node, ok := px.prepareStatus.Find(seq)
	status := node.State
	if ok {
		n_p := status.NP
		if n > n_p || status.Done {
			status.NP = args.N
			reply.Success = true
			reply.N_A = status.NA
			reply.V_A = status.VA
		} else {
			reply.Success = false
		}
	} else {
		status.NP = args.N
		reply.Success = true
		px.prepareStatus.Append(node)
	}
	node.State = status
	return nil
}

//accept function
func (px *Paxos) AcceptHandler(args RequestArgs, reply *ResponseReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	seq := args.Seq
	n := args.N

	node, ok := px.prepareStatus.Find(seq)
	status := node.State
	if ok {
		n_p := status.NP
		if n >= n_p {
			status.NP = args.N
			status.NA = args.N
			status.VA = args.V
			reply.AcceptSuccess = true
			reply.N = args.N
		} else {
			reply.AcceptSuccess = false
		}
	} else {
		reply.AcceptSuccess = false
	}
	node.State = status
	return nil
}

//decided function
func (px *Paxos) DecidedHandler(args RequestArgs, reply *ResponseReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	seq := args.Seq
	v := args.V
	node, ok := px.prepareStatus.Find(seq)
	if ok {
		status := node.State
		status.VA = v
		status.Done = true
		node.State = status
		//done update
		//暂时不支持扩容
		px.doneMins[args.Me] = args.MinForgotten
		//delete elements
		px.Min()
	}

	return nil
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.prepareStatus = NewLinkedList()
	px.localDoneMin = -1
	px.doneMins = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		px.doneMins[i] = -1
	}
	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
