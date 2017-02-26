package raftkv

const (
	OK          = "OK"
	BadRequest  = "BadRequest"
	TimeOut     = "TimeOut"
	WrongLeader = "WrongLeader"
//	ErrNoKey   = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"

	Client int
	SeqNo  int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string

	Client int
	SeqNo  int
}

type GetReply struct {
	Err   Err
	Value string
}
