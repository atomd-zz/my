package pbservice

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongServer = "ErrWrongServer"
)

type Err string

type PutArgs struct {
  Key string
  Value string
}

type PutReply struct {
  Err Err
}

type GetArgs struct {
  Key string
}

type GetReply struct {
  Err Err
  Value string
}

// Your RPC definitions here.
type SyncArgs struct {
  Me string
}

type SyncReply struct {
  KVStore map[string]string
  Err Err
}

type ForwardArgs PutArgs
type ForwardReply PutReply
