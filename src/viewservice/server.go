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

	//must be primary and  same num
	//viewNum := args.Viewnum
	// Your code here.
	// there is no backup and there is an idle server

	if vs.ServerPingTime == nil {
		vs.ServerPingTime = make(map[string]time.Time)
	}
	vs.ServerPingTime[args.Me] = time.Now()

	if &vs.View == nil {
		vs.View = *new(View)
	}
	view := &vs.View

	if view.Primary == "" && view.Backup == "" {
		//初次注册,可以直接设置并且更改view num
		view.Primary = args.Me
		view.Viewnum = view.Viewnum + 1
		vs.acked = false
		reply.View = *view
		return nil
	}

	//crashed
	if view.Primary == args.Me && args.Viewnum == 0 {
		vs.PrimaryCrashed = true
		//crashed  之后,再次ping0 可以放入备用
		vs.idle = args.Me
		reply.View = *view
		return nil
	}

	if vs.PrimaryCrashed && vs.BackupCrashed {
		log.Printf("all crashed")
		view.Primary = ""
		view.Backup = ""
		reply.View = *view
		return nil
	}

	log.Printf("the backup is %s and the Me is %s", view.Backup, args.Me)
	if vs.PrimaryCrashed && view.Backup == args.Me && vs.acked {
		log.Printf("did it do this p server %s", args.Me)
		view.Viewnum = view.Viewnum + 1
		view.Primary = view.Backup

		if vs.idle != "" {
			view.Backup = vs.idle
			vs.idle = ""
		} else {
			view.Backup = ""
		}
		vs.PrimaryCrashed = false
		vs.acked = false
		reply.View = *view
		return nil
	}

	if vs.BackupCrashed && view.Primary == args.Me {
		log.Printf("did it do this b server")
		view.Viewnum = view.Viewnum + 1
		if vs.idle != "" {
			view.Backup = vs.idle
			vs.idle = ""
		} else {
			view.Backup = ""
		}
		vs.BackupCrashed = false
		reply.View = *view
		return nil
	}

	if args.Me == view.Primary && args.Viewnum == view.Viewnum {
		vs.acked = true
	}

	if vs.acked {
		if args.Me != view.Primary {
			if view.Backup == "" {
				vs.acked = false
				view.Viewnum = view.Viewnum + 1
				view.Backup = args.Me
				reply.View = *view
				return nil
			}
		}

	} else {
		if args.Me != view.Primary && args.Me != view.Backup {
			vs.idle = args.Me
		}
	}
	reply.View = *view
	return nil
}

func checkCreateNewView(vs *ViewServer, args *PingArgs) bool {
	View := vs.View
	// it hasn't received recent Pings from both primary and backup
	if View.Backup == "" && View.Primary == "" {
		return true
	}
	// if the primary or backup  restarted (存在me,but the ping is 0)
	if View.Viewnum == 0 {
		return true
	}

	// if the primary or backup crashed
	if vs.pbCrashed {
		vs.pbCrashed = false
		return true
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
	View := vs.View
	if &View == nil {
		log.Printf("not server register to vs")
		return
	}
	if View.Primary == "" && View.Backup == "" {
		log.Printf("not server register to vs although the View is inited")
		return
	}

	if checkServerDead(View.Primary, vs.ServerPingTime) {
		log.Printf("p server is out of line")
		vs.PrimaryCrashed = true
		//promote the backup
	}

	if checkServerDead(View.Backup, vs.ServerPingTime) {
		log.Printf("b server is out of line")
		vs.BackupCrashed = true
	}

	if checkServerDead(vs.idle, vs.ServerPingTime) {
		vs.idle = ""
	}
}

func checkServerDead(me string, recent map[string]time.Time) bool {
	if me == "" {
		return false
	}
	recentTime, ok := recent[me]
	if !ok {
		log.Printf("we have not find the recent ping of %s from map", me)
		return false
	}
	//log.Printf("the recent ping is %s", recentTime.Format(time.RFC3339))
	duration := time.Since(recentTime)
	//log.Printf("the duration is %d", duration.Nanoseconds())
	limit := DeadPings * PingInterval

	//log.Printf("the limit time is %f,the duration time is %f", limit.Seconds(), duration.Seconds())
	if limit.Seconds() > duration.Seconds() {
		return false
	} else {
		//log.Printf("the server %s is overtime, so it will be removed", me)
		return true
	}

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
