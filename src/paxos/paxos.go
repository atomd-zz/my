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
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "time"

type ProposerState struct {
  mu sync.Mutex
}

type LearnerState struct {
  v interface{}
  decided bool
}

type AcceptorState struct {
  n_p int
  n_a int
  v_a interface{}
}

type Instance struct {
  seq int
  ps ProposerState
  as AcceptorState
  ls LearnerState
}

type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]

  // Your data here.
  instances map[int]*Instance // instance info
  dones []int
  max_seq int
}


// RPC
type PrepareReq struct {
  Seq int
  N int
}

type PrepareRsp struct {
  OK bool
  N int
  V interface{}
}

type AcceptReq struct {
  Seq int
  N int
  V interface{}
}

type AcceptRsp struct {
  OK bool
  N int
}

type DecidedReq struct {
  Seq int
  V interface{}
}

type DecidedRsp struct {
  Done int
}

func (px *Paxos) propose(ins *Instance, v *interface{}, n_h int) {

  lp := len(px.peers)
  seq := ins.seq

  ins.ps.mu.Lock()
  defer ins.ps.mu.Unlock()

  // while not decided
  for {
    // choose n, unique and higher than any n seen so far
    n := (n_h/lp + 1) * lp + px.me + 1
    n_max := 0
    promised := 0
    connected := 0
    v_a := v
    pre_req := &PrepareReq{N: n, Seq: seq}
    var pre_rsp PrepareRsp
    // send prepare(n) to all servers including self
    for i := range px.peers {
      if ok := call(px.peers[i], "Paxos.Prepare", pre_req, &pre_rsp); ok {
        connected++
        if pre_rsp.OK {
          promised++
          if pre_rsp.N > n_max {
            n_max = pre_rsp.N
            v_a = &pre_rsp.V
          }
        } else {
          if pre_rsp.N > n_h {
             n_h = pre_rsp.N
          }
        }
      }
    }

    if promised <= lp/2 {
      if connected <= lp/2 {
        time.Sleep(5 * time.Millisecond)
      }
      continue
    }
    // if prepare_ok(n_a, v_a) from majority,
    // v' = v_a with highest n_a; choose own v otherwise
    // and then send accept(n, v') to all
    promised = 0
    connected = 0 
    acc_req  := &AcceptReq{N: n, V: *v_a, Seq: seq}
    var acc_rsp PrepareRsp
    for i := range px.peers {
      if ok := call(px.peers[i], "Paxos.Accept", acc_req, &acc_rsp); ok {
        connected++
        if acc_rsp.OK { //&& n == acc_rsp.N  {
          if n == acc_rsp.N {
             log.Printf("#Check rsp == n")
          }
          log.Printf("#Check seq:%d rsp.N:%d n:%d", seq, acc_req.N, n)
          promised++
        } else {
          if acc_rsp.N > n_h {
             n_h = acc_rsp.N
          }
        }
      }
    }

    if promised <= lp/2 {
      if connected <= lp/2 {
        time.Sleep(5 * time.Millisecond)
      }
      continue
    }

    // if accept_ok(n) from majority, send decided(v') to all
    dec_args := &DecidedReq{V: *v_a, Seq: seq}
    var dec_rsp DecidedRsp
    ndones := make([]int, lp)
    for i := range px.peers {
      if ok := call(px.peers[i], "Paxos.Decided", dec_args, &dec_rsp); ok {
        log.Printf("%d decided %d", px.me, seq)
        ndones[i] = dec_rsp.Done
      }
    }
    px.mu.Lock()
    px.dones = ndones
    px.mu.Unlock()
    break
  }
}


func (px *Paxos) instance(seq int) *Instance {
  ins, exist := px.instances[seq]
  if !exist {
    ins = new(Instance)
    px.instances[seq] = ins
    ins.seq = seq
    ins.ls.decided = false
  }
  return ins
}

func (px *Paxos) Prepare(args *PrepareReq, reply *PrepareRsp) error {

  px.mu.Lock()
  defer px.mu.Unlock()

  ins := px.instance(args.Seq)
  if args.N > ins.as.n_p {
    reply.OK = true
    reply.N = ins.as.n_a
    reply.V = ins.as.v_a
    ins.as.n_p = args.N
  } else {
    reply.OK = false
    reply.N = ins.as.n_p
  }

  return nil
}


func (px *Paxos) Accept(args *AcceptReq, reply *AcceptRsp) error {

  px.mu.Lock()
  defer px.mu.Unlock()

  ins := px.instance(args.Seq)
  if args.N >= ins.as.n_p {
    reply.OK = true
    reply.N = ins.as.n_a
    ins.as.n_a = args.N
    ins.as.n_p = args.N
    ins.as.v_a = args.V
  } else {
    reply.OK = false
    reply.N = ins.as.n_p
  }
  return nil
}


func (px *Paxos) Decided(args *DecidedReq, reply *DecidedRsp) error {

  px.mu.Lock()
  defer px.mu.Unlock()

  ins := px.instance(args.Seq)
  ins.ls.decided = true
  ins.ls.v = args.V
  reply.Done = px.dones[px.me]

  return nil
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

  if seq + 1 < px.Min() {
    return
  }

  px.mu.Lock()
  defer px.mu.Unlock()

  ins := px.instance(seq)
  if ins.ls.decided {
    return
  }

  if seq > px.max_seq {
    px.max_seq = seq
  }
  go px.propose(ins, &v, ins.as.n_p)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  px.mu.Lock()
  defer px.mu.Unlock()
  // Your code here.
  if px.dones[px.me] < seq {
    px.dones[px.me] = seq
    for  s, _ := range px.instances {
      if s <= seq {
        delete(px.instances, s)
      }
    }
  }
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  px.mu.Lock()
  defer px.mu.Unlock()
  // Your code here.
  return px.max_seq
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
// It is illegal to call Done(i) on a peer and
// then call Start(j) on that peer for any j <= i.
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
  px.mu.Lock()
  defer px.mu.Unlock()
  min_done := px.dones[0]
  for i := 1; i < len(px.dones); i++ {
    if min_done > px.dones[i] {
      min_done = px.dones[i]
    }
  }
  return min_done + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  px.mu.Lock()
  defer px.mu.Unlock()
  ins := px.instance(seq)

  if ins.ls.decided {
    return true, ins.ls.v
  }
  return false, nil
}


//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
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
  px.dones = make([]int, len(px.peers))
  px.instances = make(map[int]*Instance)
  px.max_seq = -1

  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l

    // please do not change any of the following code,
    // or do anything to subvert it.

    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }

  return px
}

