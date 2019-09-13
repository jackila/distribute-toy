package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	View           View
	idle           string
	pbCrashed      bool
	ServerPingTime map[string]time.Time
	acked          bool
	PrimaryCrashed bool
	BackupCrashed  bool
}

//
// server Ping RPC handler.
//
// 只有view num变化了 才会对primary backup 进行切换
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	// vs.mu.Lock()
	// defer vs.mu.Unlock()
	//must be primary and  same num
	//viewNum := args.Viewnum
	// Your code here.
	// there is no backup and there is an idle server
	vs.mu.Lock()
	defer vs.mu.Unlock()
	vs.ServerPingTime[args.Me] = time.Now()

	view := &vs.View
	if args.Me == view.Primary && args.Viewnum == view.Viewnum {
		vs.acked = true
	}
	me := args.Me
	switch {
	case vs.PrimaryCrashed && vs.BackupCrashed:
		//do nothing
	case view.Primary == "" && view.Backup == "":
		view.Primary = me
		view.Viewnum = view.Viewnum + 1
		vs.acked = false

	case view.Primary == me && args.Viewnum == 0:
		vs.PrimaryCrashed = true
		//crashed  之后,再次ping0 可以放入备用
		vs.idle = me

	case view.Backup == me && args.Viewnum == 0:
		vs.BackupCrashed = true
		//crashed  之后,再次ping0 可以放入备用

	case vs.PrimaryCrashed && view.Backup == me && vs.acked:
		//todo
		view.Viewnum = view.Viewnum + 1
		backUp := view.Backup
		if vs.idle != "" {
			if vs.idle != me {
				view.Backup = vs.idle
			}
			vs.idle = ""
		} else {
			view.Backup = ""
		}
		view.Primary = backUp

		vs.PrimaryCrashed = false
		vs.acked = false

	case vs.BackupCrashed && view.Primary == me:
		view.Viewnum = view.Viewnum + 1
		if vs.idle != "" {
			view.Backup = vs.idle
			vs.idle = ""
		} else {
			view.Backup = ""
		}
		vs.BackupCrashed = false

	case vs.acked && me != view.Primary && view.Backup == "":
		vs.acked = false
		view.Viewnum = view.Viewnum + 1
		if me == vs.idle {
			vs.idle = ""
		}
		view.Backup = me

	case !vs.acked && me != view.Primary && me != view.Backup:
		vs.idle = me
	}

	reply.View = *view
	return nil

}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()
	reply.View = vs.View

	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	View := vs.View
	if View.Primary == "" && View.Backup == "" {
		return
	}

	if checkServerDead(View.Primary, vs.ServerPingTime, vs.mu) {
		vs.PrimaryCrashed = true
	}

	if checkServerDead(View.Backup, vs.ServerPingTime, vs.mu) {
		vs.BackupCrashed = true
	}

	//to be reactor
	if checkServerDead(vs.idle, vs.ServerPingTime, vs.mu) {
		vs.idle = ""
	}
}

func checkServerDead(me string, recent map[string]time.Time, mu sync.Mutex) bool {
	if me == "" {
		return false
	}
	//mu.Lock()
	recentTime, ok := recent[me]
	//mu.Unlock()
	if !ok {
		log.Printf("we have not find the recent ping of %s from map", me)
		return false
	}
	duration := time.Since(recentTime)
	limit := DeadPings * PingInterval

	return limit.Seconds() <= duration.Seconds()
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.ServerPingTime = make(map[string]time.Time)
	vs.View = *new(View)
	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
