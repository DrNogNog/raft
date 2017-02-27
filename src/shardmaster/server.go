package shardmaster


import (
	"encoding/gob"
	"fmt"
	"labrpc"
	"log"
	"raft"
	"sort"
	"sync"
	"time"
)

const Debug = 0

type OpType int
const (
	Join  = 1
	Leave = 2
	Move  = 3
	Query = 4
)

type Op struct {
	Type OpType

	// Join only
	Servers map[int][]string
	// Leave only
	GIDs    []int
	// Move only
	Shard   int
	GID     int
	// Query only
	Num	    int

	Client  int
	SeqNo   int
}

type RaftOp struct {
	op     Op
	doneCh (chan interface{})
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	configs    []Config				// indexed by config num
	executedTo map[int]int			// sequence number of request the client is executed to
	lastResult map[int]Config		// execution result of last request (Query only)
	ops        map[int64]*RaftOp	// pending ops, keyed by client and request sequence number

	killCh     chan struct{}
}

func (sm *ShardMaster) DPrintf(format string, a ...interface{}) {
	if Debug > 0 {
		log.SetPrefix(fmt.Sprintf("[SM][%d] ", sm.me))
		log.Printf(format, a...)
	}
}

func hashClientAndSeqNo(client int, seqNo int) int64 {
	return int64(client) << 32 | int64(seqNo)
}

func (sm *ShardMaster) createOp(opType OpType, client int, seqNo int) (*RaftOp, int64) {
	op := new(RaftOp)

	op.doneCh = make(chan interface{})
	op.op.Type = opType
	op.op.Client = client
	op.op.SeqNo = seqNo
	op.op.Type = opType

	hash := hashClientAndSeqNo(client, seqNo)
	sm.ops[hash] = op

	return op, hash
}

func (sm *ShardMaster) destroyOp(op *RaftOp, hash int64) {
	select {
		case <-op.doneCh:
		default:
	}
	close(op.doneCh)
	delete(sm.ops, hash)
}

func (sm *ShardMaster) isLeader() bool {
	_, isLeader := sm.rf.GetState()
	return isLeader
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	if !sm.isLeader() {
		reply.Err = WrongLeader
		return
	}
	reply.Err = OK

	sm.mu.Lock()

	// TODO(foreverbell): Be more correct.
	if sm.executedTo[args.Client] >= args.SeqNo {
		sm.mu.Unlock()
		return
	}

	op, hash := sm.createOp(Join, args.Client, args.SeqNo)
	op.op.Servers = args.Servers

	sm.mu.Unlock()

	logIndex, _, _ := sm.rf.Start(op.op)

	sm.DPrintf("[Join] From client %d, seqno = %d, log = %d.\n", args.Client, args.SeqNo, logIndex)

	timer := time.NewTimer(time.Duration(300) * time.Millisecond)
	select {
		case <- timer.C: {
			sm.DPrintf("[Join] TimeOut. Client %d, seqno = %d.\n", args.Client, args.SeqNo)
			reply.Err = TimeOut
		}
		case <-op.doneCh: {
			sm.DPrintf("[Join] Executed OK. Client %d, seqno = %d.\n", args.Client, args.SeqNo)
		}
	}

	sm.mu.Lock()
	sm.destroyOp(op, hash)
	sm.mu.Unlock()
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	if !sm.isLeader() {
		reply.Err = WrongLeader
		return
	}
	reply.Err = OK

	sm.mu.Lock()

	// TODO(foreverbell): Be more correct.
	if sm.executedTo[args.Client] >= args.SeqNo {
		sm.mu.Unlock()
		return
	}

	op, hash := sm.createOp(Leave, args.Client, args.SeqNo)
	op.op.GIDs = args.GIDs

	sm.mu.Unlock()

	logIndex, _, _ := sm.rf.Start(op.op)

	sm.DPrintf("[Leave] From client %d, seqno = %d, log = %d.\n", args.Client, args.SeqNo, logIndex)

	timer := time.NewTimer(time.Duration(300) * time.Millisecond)
	select {
		case <- timer.C: {
			sm.DPrintf("[Leave] TimeOut. Client %d, seqno = %d.\n", args.Client, args.SeqNo)
			reply.Err = TimeOut
		}
		case <-op.doneCh: {
			sm.DPrintf("[Leave] Executed OK. Client %d, seqno = %d.\n", args.Client, args.SeqNo)
		}
	}

	sm.mu.Lock()
	sm.destroyOp(op, hash)
	sm.mu.Unlock()
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	if !sm.isLeader() {
		reply.Err = WrongLeader
		return
	}
	reply.Err = OK

	sm.mu.Lock()

	// TODO(foreverbell): Be more correct.
	if sm.executedTo[args.Client] >= args.SeqNo {
		sm.mu.Unlock()
		return
	}

	op, hash := sm.createOp(Move, args.Client, args.SeqNo)
	op.op.Shard = args.Shard
	op.op.GID = args.GID

	sm.mu.Unlock()

	logIndex, _, _ := sm.rf.Start(op.op)

	sm.DPrintf("[Move] From client %d, seqno = %d, log = %d.\n", args.Client, args.SeqNo, logIndex)

	timer := time.NewTimer(time.Duration(300) * time.Millisecond)
	select {
		case <- timer.C: {
			sm.DPrintf("[Move] TimeOut. Client %d, seqno = %d.\n", args.Client, args.SeqNo)
			reply.Err = TimeOut
		}
		case <-op.doneCh: {
			sm.DPrintf("[Move] Executed OK. Client %d, seqno = %d.\n", args.Client, args.SeqNo)
		}
	}

	sm.mu.Lock()
	sm.destroyOp(op, hash)
	sm.mu.Unlock()
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	if !sm.isLeader() {
		reply.Err = WrongLeader
		return
	}
	reply.Err = OK

	sm.mu.Lock()

	// TODO(foreverbell): Be more correct.
	if sm.executedTo[args.Client] > args.SeqNo {
		reply.Err = BadRequest
		sm.mu.Unlock()
		return
	} else if sm.executedTo[args.Client] == args.SeqNo {
		reply.Config = sm.lastResult[args.Client]
		sm.mu.Unlock()
		return
	}

	op, hash := sm.createOp(Query, args.Client, args.SeqNo)
	op.op.Num = args.Num

	sm.mu.Unlock()

	logIndex, _, _ := sm.rf.Start(op.op)

	sm.DPrintf("[Query] From client %d, seqno = %d, log = %d.\n", args.Client, args.SeqNo, logIndex)

	timer := time.NewTimer(time.Duration(300) * time.Millisecond)
	select {
		case <- timer.C: {
			sm.DPrintf("[Query] TimeOut. Client %d, seqno = %d.\n", args.Client, args.SeqNo)
			reply.Err = TimeOut
		}
		case config := <-op.doneCh: {
			sm.DPrintf("[Query] Executed OK. Client %d, seqno = %d.\n", args.Client, args.SeqNo)
			reply.Config = config.(Config)
		}
	}

	sm.mu.Lock()
	sm.destroyOp(op, hash)
	sm.mu.Unlock()
}

func (sm *ShardMaster) cloneLatestConfig() Config {
	var config Config

	config_template := sm.configs[len(sm.configs) - 1]

	config.Num = config_template.Num + 1
	config.Groups = make(map[int][]string)

	for gid, servers := range(config_template.Groups) {
		config.Groups[gid] = make([]string, len(servers))
		copy(config.Groups[gid], servers)
	}

	for i := 0; i < NShards; i++ {
		config.Shards[i] = config_template.Shards[i]
	}
	return config
}

func (sm *ShardMaster) createConfig(configNum int, group map[int][]string) Config {
	var config Config
	var gids []int

	config.Num = configNum
	config.Groups = group

	for gid, _ := range group {
		gids = append(gids, gid)
	}
	sort.Ints(gids) // shitty golang does not make a promise on the key order of map enumeration.

	if len(gids) == 0 {
		for i := 0; i < NShards; i++ {
			config.Shards[i] = 0
		}
	} else {
		// Naive shards assignment.
		gidIndex := 0
		for i := 0; i < NShards; i++ {
			config.Shards[i] = gids[gidIndex]
			gidIndex += 1
			if gidIndex == len(gids) {
				gidIndex = 0
			}
		}
	}

	return config
}

func (sm *ShardMaster) executeLog(applyMsg raft.ApplyMsg) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := applyMsg.Command.(Op)

	if sm.executedTo[op.Client] > op.SeqNo {
		// This is an already committed log. Ignore.
		sm.DPrintf("Committing an already committed log %d, client = %d, seqno = %d, type = %d. Ignored.\n",
			applyMsg.Index, op.Client, op.SeqNo, op.Type)
		return
	} else if op.SeqNo - sm.executedTo[op.Client] > 1 {
		sm.DPrintf("FATAL: Missing logs client = %d, %d %d.\n", op.Client, sm.executedTo[op.Client], op.SeqNo)
		panic("Missing logs.")
	}

	alreadyCommitted := (sm.executedTo[op.Client] == op.SeqNo)

	if !alreadyCommitted {
		if op.Type == Query {
			queryIndex := op.Num

			if queryIndex == -1 || queryIndex >= len(sm.configs) {
				queryIndex = len(sm.configs) - 1
			}
			sm.lastResult[op.Client] = sm.configs[queryIndex]
		} else if op.Type == Join {
			config := sm.cloneLatestConfig()
			for gid, servers := range(op.Servers) {
				config.Groups[gid] = servers
			}
			sm.configs = append(sm.configs, sm.createConfig(config.Num, config.Groups))
		} else if op.Type == Leave {
			config := sm.cloneLatestConfig()
			for _, gid := range(op.GIDs) {
				delete(config.Groups, gid)
			}
			sm.configs = append(sm.configs, sm.createConfig(config.Num, config.Groups))
		} else if op.Type == Move {
			config := sm.cloneLatestConfig()
			config.Shards[op.Shard] = op.GID
			sm.configs = append(sm.configs, config)
		}
	}

	sm.executedTo[op.Client] = op.SeqNo

	pendingOp, was := sm.ops[hashClientAndSeqNo(op.Client, op.SeqNo)]
	if was {
		var value interface{}
		if op.Type == Query {
			value = sm.lastResult[op.Client]
		} else {
			value = struct{}{}
		}
		select {
			case pendingOp.doneCh <- value:
			default:
		}
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	sm.killCh <- struct{}{}
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	sm.executedTo = make(map[int]int)
	sm.lastResult = make(map[int]Config)
	sm.ops = make(map[int64]*RaftOp)

	sm.killCh = make(chan struct{})

	sm.DPrintf("Shardmaster server is up.\n")

	go func() {
		for {
			select {
				case <-sm.killCh: {
					return
				}
				case applyMsg := <-sm.applyCh: {
					sm.executeLog(applyMsg)
				}
			}
		}
	}()

	return sm
}
