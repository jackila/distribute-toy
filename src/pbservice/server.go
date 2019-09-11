package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	view  viewservice.View
	store map[string]string
	XIDS  map[int64]string
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.

	value, _ := pb.store[args.Key]
	reply.Value = value
	//log.Printf("the me is %s,the stroe is %v", pb.me, pb.store)
	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	//log.Printf("now the request is %+v", args)
	//log.Printf("current request is %s,and the store is %+v", pb.me, pb.store)
	key := args.XID
	_, ok := pb.XIDS[key]
	if ok {
		return nil
	}

	v := args.Value
	switch args.Type {
	case "Put":
		v = args.Value
	case "Append":
		value, ok := pb.store[args.Key]
		if !ok {
			v = args.Value
		} else {
			v = value + args.Value
		}
	}
	pb.XIDS[args.XID] = v
	pb.store[args.Key] = v
	//如果backup存在,将数据保存在backup中
	//
	if pb.me == pb.view.Primary && pb.view.Backup != "" {
		backArgs := args
		backReply := new(PutAppendReply)
		backArgs.Value = v
		call(pb.view.Backup, "PBServer.PutAppend", &backArgs, &backReply)
		//log.Printf("成功将数据同步到backup中:%t", success)
	}

	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.
	viewNum := uint(0)
	if &pb.view != nil {
		viewNum = pb.view.Viewnum
	}
	vclerk := pb.vs
	view, _ := vclerk.Ping(viewNum)

	if pb.view.Primary == pb.me && pb.view.Backup == "" && view.Backup != "" {
		//log.Printf("数据同步,p to b")
		//send the store to backup
		for key, value := range pb.store {
			backArgs := new(PutAppendArgs)
			backReply := new(PutAppendReply)
			backArgs.Key = key
			backArgs.Value = value
			backArgs.Type = "Put"
			call(view.Backup, "PBServer.PutAppend", &backArgs, &backReply)
		}
	}
	pb.view = view
	// log.Printf("now the pb view is %+v", pb.view)
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	//从primary中复制数据
	pb.store = make(map[string]string)
	pb.XIDS = make(map[int64]string)
	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
