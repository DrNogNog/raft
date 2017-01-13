package raftkv

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

type OpType int
const (
	Get    = 1
	Put    = 2
	Append = 3
)

type Op struct {
	Type      OpType
	Key       string
	Value     string  // value is returned here for Get

	Client    int
	ReqSerial int
}

type RaftOp struct {
	op Op
	value  string  // Get only
	doneCh (chan struct{})
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxRaftState int // snapshot if log grows this big
	snapshotting bool

	kv         map[string]string
	executedTo map[int]int        // serial number of request the client is executed to
	lastResult map[int]string     // execution result of last request
	ops        map[int64]*RaftOp  // pending ops, keyed by client and request serial number

	killed     bool
}

func (kv *RaftKV) DPrintf(format string, a ...interface{}) {
	if Debug > 0 && !kv.killed {
		log.SetPrefix(fmt.Sprintf("[KV][%d] ", kv.me))
		log.Printf(format, a...)
	}
}

func hashClientAndSerial(client int, serial int) int64 {
	return int64(client) << 32 | int64(serial)
}

func (kv *RaftKV) Get(args GetArgs, reply *GetReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = OK

	kv.mu.Lock()

	// Check if this operation has already been executed.
	if kv.executedTo[args.Client] > args.ReqSerial {
		// As there is only one client, and the client's request is executed to at least
		// kv.executedTo, but server finds client is doing request with serial < kv.executedTo.
		// ? ? ?
		reply.Err = BadRequest
		kv.mu.Unlock()
		return
	} else if kv.executedTo[args.Client] == args.ReqSerial {
		reply.Value = kv.lastResult[args.Client]
		kv.mu.Unlock()
		return
	}

	op := new(RaftOp)

	op.doneCh = make(chan struct{})
	op.op.Client = args.Client
	op.op.Key = args.Key
	op.op.Type = Get
	op.op.ReqSerial = args.ReqSerial

	hash := hashClientAndSerial(op.op.Client, op.op.ReqSerial)
	kv.ops[hash] = op

	kv.mu.Unlock()

	logIndex, _, _ := kv.rf.Start(op.op)

	kv.DPrintf("[Get] From client %d, key = %s, serial = %d, log = %d.\n", args.Client, args.Key, args.ReqSerial, logIndex)

	// Timer is necessary as this log can never be committed if server loses leadership.
	timer := time.NewTimer(time.Duration(300) * time.Millisecond)

	select {
		case <- timer.C: {
			kv.DPrintf("[Get] TimeOut. Client %d, key = %s, serial = %d.\n", args.Client, args.Key, args.ReqSerial)
			reply.Err = TimeOut
		}
		case <-op.doneCh: {
			kv.DPrintf("[Get] Executed OK. Client %d, key = %s, serial = %d.\n", args.Client, args.Key, args.ReqSerial)
			kv.mu.Lock()
			reply.Value = op.value
			kv.mu.Unlock()
		}
	}

	kv.mu.Lock()
	// FIXME: Find a more elegant way to synchronize between go routines, as close a channel in
	// receiver is not recommended in golang.
	select {
		case <-op.doneCh:
		default:
	}
	close(op.doneCh)
	delete(kv.ops, hash)
	kv.mu.Unlock()
}

func (kv *RaftKV) PutAppend(args PutAppendArgs, reply *PutAppendReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	reply.Err = OK

	kv.mu.Lock()

	// Check if this operation has already been executed.
	if kv.executedTo[args.Client] >= args.ReqSerial {
		kv.mu.Unlock()
		return
	}

	op := new(RaftOp)

	op.doneCh = make(chan struct{})
	op.op.Client = args.Client
	op.op.Key = args.Key
	op.op.Value = args.Value
	if args.Op == "Put" {
		op.op.Type = Put
	} else {
		op.op.Type = Append
	}
	op.op.ReqSerial = args.ReqSerial

	hash := hashClientAndSerial(op.op.Client, op.op.ReqSerial)
	kv.ops[hash] = op

	kv.mu.Unlock()

	logIndex, _, _ := kv.rf.Start(op.op)

	kv.DPrintf("[%s] From client %d, key = %s, value = %s, serial = %d, log = %d.\n", args.Op, args.Client, args.Key, args.Value, args.ReqSerial, logIndex)

	// Timer is necessary as this log can never be committed if server loses its leadership.
	timer := time.NewTimer(time.Duration(250) * time.Millisecond)

	select {
		case <- timer.C: {
			kv.DPrintf("[%s] TimeOut. Client %d, key = %s, value = %s, serial = %d.\n", args.Op, args.Client, args.Key, args.Value, args.ReqSerial)
			reply.Err = TimeOut
		}
		case <-op.doneCh: {
			kv.DPrintf("[%s] Executed OK. Client %d, key = %s, value = %s, serial = %d.\n", args.Op, args.Client, args.Key, args.Value, args.ReqSerial)
		}
	}

	kv.mu.Lock()
	// FIXME: Find a more elegant way to synchronize between go routines, as close a channel in
	// receiver is not recommended in golang.
	select {
		case <-op.doneCh:
		default:
	}
	close(op.doneCh)
	delete(kv.ops, hash)
	kv.mu.Unlock()
}

func (kv *RaftKV) execute(applyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := applyMsg.Command.(Op)

	if kv.executedTo[op.Client] > op.ReqSerial {
		// This is an already committed log. Ignore.
		kv.DPrintf("Committing an already committed log %d, client = %d, serial = %d, %d %s %s. Ignored.\n", applyMsg.Index, op.Client, op.ReqSerial, op.Type, op.Key, op.Value)
		return
	} else if op.ReqSerial - kv.executedTo[op.Client] > 1 {
		kv.DPrintf("FATAL: Missing logs client = %d, %d %d.\n", op.Client, kv.executedTo[op.Client], op.ReqSerial)
		panic("Missing logs.")
	}

	alreadyCommitted := (kv.executedTo[op.Client] == op.ReqSerial)

	if !alreadyCommitted {
		if op.Type == Get {
			kv.lastResult[op.Client] = kv.kv[op.Key]
		} else if op.Type == Append {
			kv.kv[op.Key] += op.Value
		} else if op.Type == Put {
			kv.kv[op.Key] = op.Value
		}
	}

	kv.executedTo[op.Client] = op.ReqSerial

	pendingOp, was := kv.ops[hashClientAndSerial(op.Client, op.ReqSerial)]
	if was {
		if op.Type == Get {
			pendingOp.value = kv.lastResult[op.Client]
		}
		select {
			case pendingOp.doneCh <- struct{}{}:
			default:
		}
	}

	if !kv.snapshotting && kv.maxRaftState != -1 && kv.rf.Persister().RaftStateSize() > kv.maxRaftState {
		kv.DPrintf("Doing snapshot until log %d.\n", applyMsg.Index)
		kv.snapshotting = true
		kv.snapshot()
		go func(index int, term int) {
			kv.rf.DiscardLogs(index, term)
			kv.mu.Lock()
			kv.snapshotting = false
			kv.mu.Unlock()
		}(applyMsg.Index, applyMsg.Term)
	}
}

func (kv *RaftKV) snapshot() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.kv)
	e.Encode(kv.executedTo)
	e.Encode(kv.lastResult)
	data := w.Bytes()
	kv.rf.Persister().SaveSnapshot(data)
}

func (kv *RaftKV) readSnapshot(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&kv.kv)
	d.Decode(&kv.executedTo)
	d.Decode(&kv.lastResult)
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	kv.killed = true
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxRaftState int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxRaftState = maxRaftState
	kv.snapshotting = false

	kv.kv = make(map[string]string)
	kv.executedTo = make(map[int]int)
	kv.lastResult = make(map[int]string)
	kv.ops = make(map[int64]*RaftOp)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.readSnapshot(kv.rf.Persister().ReadSnapshot())

	kv.DPrintf("KV server is up, kv = %v.\n", kv.kv)

	go func() {
		for {
			applyMsg := <-kv.applyCh

			if applyMsg.UseSnapshot {
				kv.mu.Lock()
				kv.readSnapshot(applyMsg.Snapshot)
				kv.snapshot()
				go func(index int, term int) {
					kv.rf.DiscardLogs(index, term)
				}(applyMsg.Index, applyMsg.Term)
				kv.mu.Unlock()
				kv.DPrintf("Snapshot reloaded to log index = %d.\n", applyMsg.Index)
			} else {
				kv.execute(applyMsg)
			}
		}
	}()

	return kv
}
