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
	idle           []string
	ServerPingTime map[string]time.Time
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// it hasn't received recent Pings from both primary and backup
	// if the primary or backup crashed and restarted (存在me,but the ping is 0)
	// there is no backup and there is an idle server

	//must be primary and  same num
	viewNum := args.Viewnum
	// Your code here.

	if &vs.View == nil {
		vs.View = *new(View)
	}
	view := &vs.View

	if checkCreateNewView(vs, args) && args.Me == view.Primary {
		view.Viewnum = view.Viewnum + 1
	}

	switch {
	case viewNum == 0 && view.Primary == "":
		view.Primary = args.Me
		view.Viewnum = view.Viewnum + 1
	case args.Me != view.Primary && viewNum == 0:
		vs.idle = append(vs.idle, args.Me)
	case args.Me == view.Primary && len(vs.idle) != 0:
		view.Backup = vs.idle[0]
		vs.idle = vs.idle[1:]
	}

	reply.View = *view

	if vs.ServerPingTime == nil {
		vs.ServerPingTime = make(map[string]time.Time)
	}
	vs.ServerPingTime[args.Me] = time.Now()
	return nil
}

func checkCreateNewView(vs *ViewServer, args *PingArgs) bool {
	View := vs.View

	if View.Backup == "" && View.Primary == "" {
		return true
	}

	if args.Me == View.Backup || args.Me == View.Primary {

		if View.Viewnum == 0 {
			return true
		}
	}

	if View.Backup == "" && len(vs.idle) > 0 {
		return true
	}

	return false
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
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
