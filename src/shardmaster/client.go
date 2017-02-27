package shardmaster

//
// Shardmaster clerk.
//

import (
	"labrpc"
	"time"
	"crypto/rand"
	"math/big"
)

type Clerk struct {
	servers []*labrpc.ClientEnd

	me      int
	seqNo   int
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

	return ck
}

func (ck *Clerk) increaseSeqNo() {
	ck.seqNo += 1
}

func (ck *Clerk) Query(num int) Config {
	defer ck.increaseSeqNo()

	var args QueryArgs
	args.Num = num
	args.Client = ck.me
	args.SeqNo = ck.seqNo

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", &args, &reply)
			// TODO: Only try another server if reply.Err == WrongLeader.
			if ok && reply.Err == OK {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	defer ck.increaseSeqNo()

	var args JoinArgs
	args.Servers = servers
	args.Client = ck.me
	args.SeqNo = ck.seqNo

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", &args, &reply)
			if ok && reply.Err == OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	defer ck.increaseSeqNo()

	var args LeaveArgs
	args.GIDs = gids
	args.Client = ck.me
	args.SeqNo = ck.seqNo

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", &args, &reply)
			if ok && reply.Err == OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	defer ck.increaseSeqNo()

	var args MoveArgs
	args.Shard = shard
	args.GID = gid
	args.Client = ck.me
	args.SeqNo = ck.seqNo

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", &args, &reply)
			if ok && reply.Err == OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
