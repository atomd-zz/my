package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  me string

  // Your declarations here.
  cv View
  fv View
  ack uint
  pings map[string] time.Time
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

  vs.mu.Lock()
  defer vs.mu.Unlock()


  if vs.cv.Primary == args.Me && vs.cv.Viewnum != args.Viewnum {
    delete(vs.pings, args.Me)
  } else {
    vs.pings[args.Me] = time.Now()
  }

  if vs.cv.Primary == args.Me && vs.cv.Viewnum == args.Viewnum {
    vs.ack = args.Viewnum
  }

  // If the view service has not yet received an acknowledgment for the current
  // view from the primary of the current view, the view service should not
  // change views even if it thinks that the primary or backup has died.
  if vs.fv.Viewnum == vs.cv.Viewnum + 1 && vs.ack == vs.cv.Viewnum &&
      vs.fv.Primary == args.Me && args.Viewnum == vs.ack {
    vs.cv = vs.fv
  }

  reply.View = vs.cv

  //log.Printf("[viewserver][ping] num:%d me:%s", args.Viewnum, args.Me)
  return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

  vs.mu.Lock()
  defer vs.mu.Unlock()

  reply.View = vs.cv

  return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

  vs.mu.Lock()
  defer vs.mu.Unlock()
  nv := vs.cv

  for s, t := range(vs.pings) {
    if time.Since(t) > PingInterval * DeadPings {
      delete(vs.pings, s)
    }
  }

  _, pexists := vs.pings[nv.Primary]
  _, bexists := vs.pings[nv.Backup]

  if pexists && bexists {
    return
  } else if !pexists && bexists {
    nv.Primary, nv.Backup = nv.Backup, "" // try to promote backup server
    pexists, bexists = true, false
  }

  once := pexists
  for s := range(vs.pings) {
    if !once {
      nv.Primary = s
      once = true
    } else if s != nv.Primary {
      nv.Backup= s
      break
    }
  }

  if (pexists || bexists || len(vs.cv.Primary) == 0) && vs.cv != nv {
    vs.fv = View{vs.cv.Viewnum + 1, nv.Primary, nv.Backup}
  }

  //log.Printf("[viewserver][tick] current num:%d primary:%s backup:%s", vs.cv.Viewnum, vs.cv.Primary, vs.cv.Backup)
  //log.Printf("[ViewServer][tick] future  num:%d primary:%s backup:%s", vs.fv.Viewnum, vs.fv.Primary, vs.fv.Backup)
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
  vs.dead = true
  vs.l.Close()
}

func StartServer(me string) *ViewServer {
  vs := new(ViewServer)
  vs.me = me
  vs.cv = View{}
  vs.fv = View{}
  vs.ack = 0
  vs.pings = map[string]time.Time{}

  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  vs.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.dead == false {
      conn, err := vs.l.Accept()
      if err == nil && vs.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      }
      if err != nil && vs.dead == false {
        fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
        vs.Kill()
      }
    }
  }()

  // create a thread to call tick() periodically.
  go func() {
    for vs.dead == false {
      vs.tick()
      time.Sleep(PingInterval)
    }
  }()

  return vs
}
