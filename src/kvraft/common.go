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

	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Client    int
	ReqSerial int
}

type PutAppendReply struct {
	Err         Err
}

type GetArgs struct {
	Key string

	// You'll have to add definitions here.
	Client    int
	ReqSerial int
}

type GetReply struct {
	Err         Err
	Value       string
}
