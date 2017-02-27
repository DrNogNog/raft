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
	Value     string  // Put and Append only

	Client    int
	SeqNo     int
}

type RaftOp struct {
	op     Op
	doneCh (chan interface{})
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxRaftState int // snapshot if log grows this big
	snapshotting bool

	kv         map[string]string
	executedTo map[int]int        // sequence number of request the client is executed to
	lastResult map[int]string     // execution result of last request (Get only)
	ops        map[int64]*RaftOp  // pending ops, keyed by client and request sequence number

	killCh     chan struct{}
}

func (kv *RaftKV) DPrintf(format string, a ...interface{}) {
	if Debug > 0 {
		log.SetPrefix(fmt.Sprintf("[KV][%d] ", kv.me))
		log.Printf(format, a...)
	}
}

func hashClientAndSeqNo(client int, seqNo int) int64 {
	return int64(client) << 32 | int64(seqNo)
}

func (kv *RaftKV) createOp(opType OpType, client int, seqNo int) (*RaftOp, int64) {
	op := new(RaftOp)

	op.doneCh = make(chan interface{})
	op.op.Type = opType
	op.op.Client = client
	op.op.SeqNo = seqNo
	op.op.Type = opType

	hash := hashClientAndSeqNo(client, seqNo)
	kv.ops[hash] = op

	return op, hash
}

func (kv *RaftKV) destroyOp(op *RaftOp, hash int64) {
	select {
		case <-op.doneCh:
		default:
	}
	close(op.doneCh)
	delete(kv.ops, hash)
}

func (kv *RaftKV) isLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}

func (kv *RaftKV) Get(args GetArgs, reply *GetReply) {
	if !kv.isLeader() {
		reply.Err = WrongLeader
		return
	}
	reply.Err = OK

	kv.mu.Lock()

	// Check if this operation has already been executed.
	if kv.executedTo[args.Client] > args.SeqNo {
		// As there is only one client, and the client's request is executed to at least
		// kv.executedTo, but server finds client is doing request with seqNo < kv.executedTo.
		// ? ? ?
		reply.Err = BadRequest
		kv.mu.Unlock()
		return
	} else if kv.executedTo[args.Client] == args.SeqNo {
		reply.Value = kv.lastResult[args.Client]
		kv.mu.Unlock()
		return
	}

	op, hash := kv.createOp(Get, args.Client, args.SeqNo)
	op.op.Key = args.Key

	kv.mu.Unlock()

	logIndex, _, _ := kv.rf.Start(op.op)

	kv.DPrintf("[Get] From client %d, key = %s, seqno = %d, log = %d.\n",
		args.Client, args.Key, args.SeqNo, logIndex)

	// Timer is necessary as this log can never be committed if server loses leadership.
	timer := time.NewTimer(time.Duration(300) * time.Millisecond)
	select {
		case <- timer.C: {
			kv.DPrintf("[Get] TimeOut. Client %d, key = %s, seqno = %d.\n",
				args.Client, args.Key, args.SeqNo)
			reply.Err = TimeOut
		}
		case value := <-op.doneCh: {
			kv.DPrintf("[Get] Executed OK. Client %d, key = %s, seqno = %d.\n",
				args.Client, args.Key, args.SeqNo)
			reply.Value = value.(string)
		}
	}

	kv.mu.Lock()
	kv.destroyOp(op, hash)
	kv.mu.Unlock()
}

func (kv *RaftKV) PutAppend(args PutAppendArgs, reply *PutAppendReply) {
	if !kv.isLeader() {
		reply.Err = WrongLeader
		return
	}
	reply.Err = OK

	kv.mu.Lock()

	// Check if this operation has already been executed.
	if kv.executedTo[args.Client] >= args.SeqNo {
		kv.mu.Unlock()
		return
	}

	var opType OpType
	if args.Op == "Put" {
		opType = Put
	} else {
		opType = Append
	}
	op, hash := kv.createOp(opType, args.Client, args.SeqNo)
	op.op.Key = args.Key
	op.op.Value = args.Value

	kv.mu.Unlock()

	logIndex, _, _ := kv.rf.Start(op.op)

	kv.DPrintf("[%s] From client %d, key = %s, value = %s, seqno = %d, log = %d.\n",
		args.Op, args.Client, args.Key, args.Value, args.SeqNo, logIndex)

	// Timer is necessary as this log can never be committed if server loses its leadership.
	timer := time.NewTimer(time.Duration(250) * time.Millisecond)
	select {
		case <- timer.C: {
			kv.DPrintf("[%s] TimeOut. Client %d, key = %s, value = %s, seqno = %d.\n",
				args.Op, args.Client, args.Key, args.Value, args.SeqNo)
			reply.Err = TimeOut
		}
		case <-op.doneCh: {
			kv.DPrintf("[%s] Executed OK. Client %d, key = %s, value = %s, seqno = %d.\n",
				args.Op, args.Client, args.Key, args.Value, args.SeqNo)
		}
	}

	kv.mu.Lock()
	kv.destroyOp(op, hash)
	kv.mu.Unlock()
}

func (kv *RaftKV) executeLog(applyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := applyMsg.Command.(Op)

	if kv.executedTo[op.Client] > op.SeqNo {
		// This is an already committed log. Ignore.
		kv.DPrintf("Committing an already committed log %d, client = %d, seqno = %d, %d %s %s. Ignored.\n",
			applyMsg.Index, op.Client, op.SeqNo, op.Type, op.Key, op.Value)
		return
	} else if op.SeqNo - kv.executedTo[op.Client] > 1 {
		kv.DPrintf("FATAL: Missing logs client = %d, %d %d.\n", op.Client, kv.executedTo[op.Client], op.SeqNo)
		panic("Missing logs.")
	}

	alreadyCommitted := (kv.executedTo[op.Client] == op.SeqNo)

	if !alreadyCommitted {
		if op.Type == Get {
			kv.lastResult[op.Client] = kv.kv[op.Key]
		} else if op.Type == Append {
			kv.kv[op.Key] += op.Value
		} else if op.Type == Put {
			kv.kv[op.Key] = op.Value
		}
	}

	kv.executedTo[op.Client] = op.SeqNo

	pendingOp, was := kv.ops[hashClientAndSeqNo(op.Client, op.SeqNo)]
	if was {
		var value interface{}
		if op.Type == Get {
			value = kv.lastResult[op.Client]
		} else {
			value = struct{}{}
		}
		select {
			case pendingOp.doneCh <- value:
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

func (kv *RaftKV) installSnapshot(applyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	kv.readSnapshot(applyMsg.Snapshot)
	kv.snapshot()
	go func(index int, term int) {
		kv.rf.DiscardLogs(index, term)
	}(applyMsg.Index, applyMsg.Term)
	kv.mu.Unlock()
	kv.DPrintf("Snapshot reloaded to log index = %d.\n", applyMsg.Index)
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
	kv.killCh <- struct{}{}
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

	kv.killCh = make(chan struct{})

	kv.readSnapshot(kv.rf.Persister().ReadSnapshot())

	kv.DPrintf("KV server is up, kv = %v.\n", kv.kv)

	go func() {
		for {
			select {
				case <-kv.killCh: {
					return
				}
				case applyMsg := <-kv.applyCh: {
					if applyMsg.UseSnapshot {
						kv.installSnapshot(applyMsg)
					} else {
						kv.executeLog(applyMsg)
					}
				}
			}
		}
	}()

	return kv
}
