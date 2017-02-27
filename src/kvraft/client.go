package raftkv

import (
	"labrpc"
	"crypto/rand"
	"math/big"
)

type Clerk struct {
	servers []*labrpc.ClientEnd

	me         int  // client id
	seqNo      int  // sequence number for next request
	lastLeader int  // last cached leader
}

func nrand() int {
	max := big.NewInt(int64(1) << 30)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return int(x)
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers

	ck.me = nrand()
	ck.seqNo = 1
	ck.lastLeader = -1

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//

func (ck *Clerk) increaseSeqNo() {
	ck.seqNo += 1
}

func (ck *Clerk) Get(key string) string {
	defer ck.increaseSeqNo()

	var get = func(i int) (GetReply, bool) {
		var args GetArgs
		var reply GetReply

		args.Key = key
		args.Client = ck.me
		args.SeqNo = ck.seqNo

		if ck.servers[i].Call("RaftKV.Get", args, &reply) && reply.Err == OK {
			ck.lastLeader = i
			return reply, true
		}
		if reply.Err == BadRequest {
			panic("bad request")
		}
		ck.lastLeader = -1
		return reply, false
	}

	for {
		var reply GetReply
		var success bool

		if ck.lastLeader != -1 {
			reply, success = get(ck.lastLeader)
		} else {
			// No cached leader found, search for the leader by enumerating.
			for i := range ck.servers {
				reply, success = get(i)
				if success {
					break
				}
			}
		}
		if success {
			return reply.Value
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	defer ck.increaseSeqNo()

	var putappend = func(i int) (PutAppendReply, bool) {
		var args PutAppendArgs
		var reply PutAppendReply

		args.Key = key
		args.Value = value
		args.Op = op
		args.Client = ck.me
		args.SeqNo = ck.seqNo

		if ck.servers[i].Call("RaftKV.PutAppend", args, &reply) && reply.Err == OK {
			ck.lastLeader = i
			return reply, true
		}
		if reply.Err == BadRequest {
			panic("bad request")
		}
		ck.lastLeader = -1
		return reply, false
	}

	for {
		var success bool

		if ck.lastLeader != -1 {
			_, success = putappend(ck.lastLeader)
		} else {
			// No cached leader found, search for the leader by enumerating.
			for i := range ck.servers {
				_, success = putappend(i)
				if success {
					break
				}
			}
		}
		if success {
			break
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
