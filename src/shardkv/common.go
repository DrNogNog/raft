package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK          = "OK"
	BadRequest  = "BadRequest"
	TimeOut     = "TimeOut"
	WrongLeader = "WrongLeader"
	WrongGroup  = "WrongGroup"
//	ErrNoKey    = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"

	Client int
	SeqNo  int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string

	Client int
	SeqNo  int
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

// XXX
type MigrateArgs struct {

}

type MigrateReply struct {

}
