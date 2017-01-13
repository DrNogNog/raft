package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"labrpc"
	"log"
	"math/rand"
	"sync"
	"time"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Term        int
	Command     interface{}
	UseSnapshot bool
	Snapshot    []byte
}

type Log struct {
	Index   int // log index
	Term    int // the term when the entry was received by leader
	Command interface{}
}

type Role int
const (
	RoleLeader    = 1
	RoleFollower  = 2
	RoleCandidate = 3
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int  // index into peers[]

	role        Role  // role
	currentTerm int   // latest term server has seen
	voteFor     int   // candidateId that received vote in currentTerm, -1 if no vote.
	                  // we should also update <voteFor> if <currentTerm> has changed.
	log	        []Log // logs, log[0] stores the term and index of last discarded log
	commitIndex int   // index of highest log entry known to be commited
	startIndex  int   // offset to the discarded logs
	                  // logs = [ 0, startIndex + 1, startIndex + 2, ... ]
					  // invariant: commitIndex >= startIndex

	killCh      chan struct{}  // killed
	downRoleCh  chan struct{}  // down to follower if set
	heartBeatCh chan HeartBeat // heartbeats
	applyCh     chan ApplyMsg  // log is applied

	// leader only state, nextIndex[leader] and matchIndex[leader] is undefined.
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server
}

type HeartBeat struct {
	leaderId     int
	term         int
	leaderCommit int
}

// Debugging
const Debug = 0

func (rf *Raft) DPrintf(format string, a ...interface{}) {
	if Debug > 0 {
		log.SetPrefix(fmt.Sprintf("[%d] ", rf.me))
		log.Printf(format, a...)
	}
}

// return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == RoleLeader
}

func (rf *Raft) Persister() *Persister {
	return rf.persister
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.startIndex)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.voteFor)
	d.Decode(&rf.startIndex)
	d.Decode(&rf.log)

	rf.commitIndex = rf.startIndex
}

//
// RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term        int  // candidate's vote
	CandidateId int  // candidate requesting for vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	FollowerId    int
	Term        int  // current term, for candidate to update itself
	VoteGranted bool // true means candidate receives vote
}

//
// AppendEntries RPC arguments structure.
//
type AppendEntriesArgs struct {
	IsHeartBeat bool // true if this RPC is a heart beat, only contains LeaderId and Term if true

	LeaderId    int  // leader's id
	Term        int  // leader's term

	PrevLogIndex int   // index of log entry immediately preceding new ones
	PrevLogTerm  int   // term of PrevLogIndex
	LeaderCommit int   // leader's commitIndex
	Entries      []Log // log entries to store
}

//
// AppendEntries RPC reply structure.
//
type AppendEntriesReply struct {
	FollowerId  int
	Term        int  // currentTerm, for leader to update itself
	Success     bool // for log consistency check
	NextIndex   int  // possible PrevLogIndex for next AppendEntries RPC
}

//
// InstallSnapshot RPC arguments structure.
//
type InstallSnapshotArgs struct {
	LeaderId int  // leader's id
	Term     int  // leader's term

	LastIncludedIndex int   // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int   // term of LastIncludedIndex
	Snapshot          []byte
}

//
// InstallSnapshot RPC reply structure.
//
type InstallSnapshotReply struct {
	Success    bool

	FollowerId int
	Term       int  // currentTerm, for leader to update itself
}

//
// Send a RequestVote RPC to a server.
// Returns true if labrpc says the RPC was delivered.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

//
// Send an AppendEntries RPC to a server.
//
func (rf* Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

//
// Send a InstallSnapshot RPC to a server.
//
func (rf* Raft) sendInstallSnapshot(server int, args InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}

func (rf *Raft) setCurrentTerm(currentTerm int) {
	if currentTerm != rf.currentTerm {
		rf.voteFor = -1
	}
	rf.currentTerm = currentTerm
}

func (rf *Raft) downRole() {
	if rf.role != RoleFollower {
		rf.role = RoleFollower
		select {
			case rf.downRoleCh <- struct{}{}:
			default:
		}
	}
}

func (rf *Raft) lastLogIndex() (int, int) {
	return rf.log[len(rf.log) - 1].Index, rf.log[len(rf.log) - 1].Term
}

// Check if we are stale leader lag behind, then catch up and convert ourselves into follower.
func (rf *Raft) checkStaleLeader(tag string, term int, peer int) bool {
	if rf.currentTerm < term {
		rf.DPrintf("[%s] Stale leader at term %d, %d has a higher term %d, converting to follower.\n",
			tag, rf.currentTerm, peer, term)
		rf.setCurrentTerm(term)
		rf.downRole()
		return true
	}
	return false
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	rf.DPrintf("Being requested voting for %d.\n", args.CandidateId)

	reply.FollowerId = rf.me

	rf.checkStaleLeader("RequestVote", args.Term, args.CandidateId)

	// Reject request if candidate's term is stale.
	if args.Term < rf.currentTerm {
		rf.DPrintf("Reject voting for %d because candidate's term is too old.\n", args.CandidateId)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// Reject request if candidate's log is not at least up-to-date as receiver's log.
	lastLogIndex, lastLogTerm := rf.lastLogIndex()
	if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
		rf.DPrintf("Reject voting for %d because candidate's log is not at least up-to-date.\n", args.CandidateId)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if rf.voteFor == -1 {
		rf.voteFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = (rf.voteFor == args.CandidateId)
	}
	rf.DPrintf("Voting for %d while being asked to vote for %d.\n", rf.voteFor, args.CandidateId)
	reply.Term = rf.currentTerm
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	rf.checkStaleLeader("AppendEntries", args.Term, args.LeaderId)

	reply.FollowerId = rf.me
	reply.Term = rf.currentTerm
	reply.NextIndex = -1

	// Reject request if leader's term is stale.
	if args.Term < rf.currentTerm {
		rf.DPrintf("Reject append entries from %d because leader's term is too old.\n", args.LeaderId)
		reply.Success = false
		return
	}

	if args.IsHeartBeat {
		// For heartbeat, add it into channel. Heartbeat just indicates the leader is still alive and also
		// does some log consistency check.
		// If log isn't consistent, return false and let leader retry.

		rf.DPrintf("Received heart beat from leader %d.\n", args.LeaderId)
		go func() {
			var heartBeat HeartBeat
			heartBeat.leaderId = args.LeaderId
			heartBeat.term = args.Term
			heartBeat.leaderCommit = args.LeaderCommit
			rf.heartBeatCh <- heartBeat
		}()

		ok, nextIndex := rf.checkLogConsistency(args)
		reply.Success = ok
		reply.NextIndex = nextIndex

		if !reply.Success {
			rf.DPrintf("[HeartBeat] Log is inconsistent with leader %d. Asking leader to retry.\n", args.LeaderId)
		}
	} else {
		// If log isn't consistent, return false and let leader retry.
		ok, nextIndex := rf.checkLogConsistency(args)
		if !ok {
			rf.DPrintf("[AppendEntries] Log is inconsistent with leader %d. Asking leader to retry.\n", args.LeaderId)
			reply.Success = false
			reply.NextIndex = nextIndex
			return
		}

		if len(args.Entries) != 0 {
			rf.DPrintf("Updating logs %d %d.\n", args.PrevLogIndex, len(args.Entries))

			// Delete bad logs and append new logs.
			diverge := false // if has inconsistent logs
			for i, j := 0, args.PrevLogIndex + 1; i < len(args.Entries); i++ {
				if j >= rf.startIndex + len(rf.log) {
					rf.log = append(rf.log, args.Entries[i])
				} else if j - rf.startIndex >= 1 {
					if diverge || rf.log[j - rf.startIndex].Term != args.Entries[i].Term {
						rf.log[j - rf.startIndex] = args.Entries[i]
						diverge = true
					}
				}
				j += 1 // shity golang
			}
			if diverge && rf.startIndex + len(rf.log) > args.PrevLogIndex + len(args.Entries) + 1 {
				rf.log = rf.log[0 : args.PrevLogIndex + len(args.Entries) + 1 - rf.startIndex]
			}
		}

		reply.Success = true

		// Update commit index.
		newCommitIndex := args.LeaderCommit
		lastLogIndex, _ := rf.lastLogIndex()
		if newCommitIndex > lastLogIndex {
			newCommitIndex = lastLogIndex
		}

		if rf.commitIndex < newCommitIndex {
			rf.DPrintf("Committing from %d to %d.\n", rf.commitIndex, newCommitIndex)

			for i := rf.commitIndex + 1; i <= newCommitIndex; i++ {
				var applyMsg ApplyMsg

				applyMsg.Index = i
				applyMsg.Term = rf.log[i - rf.startIndex].Term
				applyMsg.Command = rf.log[i - rf.startIndex].Command
				applyMsg.UseSnapshot = false
				rf.applyCh <- applyMsg
			}
			rf.commitIndex = newCommitIndex
		}
	}
}

// Discard all old log entries before lastLogIndex.
// Invoked by service built on Raft to discard committed logs after they are executed.
func (rf *Raft) DiscardLogs(lastLogIndex int, lastLogTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.commitIndex < lastLogIndex {
		rf.commitIndex = lastLogIndex
	}

	rf.DPrintf("Discard logs to %d.\n", lastLogIndex)

	discardFrom := lastLogIndex - rf.startIndex

	if discardFrom <= 0 {
		return
	} else if discardFrom >= len(rf.log) {
		// Commit index is too ahead, no enough logs to discard.
		rf.startIndex = lastLogIndex
		rf.log = make([]Log, 1)
		rf.log[0].Index = lastLogIndex
		rf.log[0].Term = lastLogTerm
	} else {
		retainLog := rf.log[discardFrom + 1:]
		rf.log[0].Index = rf.log[discardFrom].Index
		rf.log[0].Term = rf.log[discardFrom].Term
		rf.log = append(rf.log[0:1], retainLog...)
		rf.startIndex += discardFrom

		if lastLogIndex != 0 && (rf.log[0].Index != lastLogIndex || rf.log[0].Term != lastLogTerm) {
			rf.log = make([]Log, 1)
			rf.log[0].Index = lastLogIndex
			rf.log[0].Term = lastLogTerm
		}
	}
}

//
// InstallSnapshot RPC handler.
//
func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	rf.checkStaleLeader("InstallSnapshot", args.Term, args.LeaderId)

	reply.FollowerId = rf.me
	reply.Term = rf.currentTerm
	reply.Success = false

	// Reject request if leader's term is stale.
	if args.Term < rf.currentTerm {
		rf.DPrintf("Reject install snapshot from %d because leader's term is too old.\n", args.LeaderId)
		return
	}

	reply.Success = true

	if args.LastIncludedIndex <= rf.commitIndex {
		rf.DPrintf("Stale InstallSnapshot from %d, lastIncludedIndex = %d, commitIndex = %d.\n",
			args.LeaderId, args.LastIncludedIndex, rf.commitIndex)
		return
	}

	rf.DPrintf("Apply snapshot, commitIndex %d -> %d.\n", rf.commitIndex, args.LastIncludedIndex)

	var applyMsg ApplyMsg

	applyMsg.UseSnapshot = true
	applyMsg.Index = args.LastIncludedIndex
	applyMsg.Term = args.LastIncludedTerm
	applyMsg.Snapshot = args.Snapshot
	rf.applyCh <- applyMsg
}

// Consistent log should contain the entry at PrevLogIndex whose term matches PrevLogTerm.
func (rf *Raft) checkLogConsistency(args AppendEntriesArgs) (bool, int) {
	lastLogIndex, _ := rf.lastLogIndex()
	if lastLogIndex < args.PrevLogIndex {
		// Missing some logs.
		return false, lastLogIndex
	}
	if args.PrevLogIndex <= rf.commitIndex {
		// Committed log must be consistent.
		return true, -1
	}
	if rf.log[args.PrevLogIndex - rf.startIndex].Term != args.PrevLogTerm {
		matchIndex := args.PrevLogIndex - 1
		// Skip all logs whose term == rf.log[args.PrevLogIndex].Term.
		for ; matchIndex > rf.startIndex; matchIndex-- {
			if rf.log[args.PrevLogIndex - rf.startIndex].Term != rf.log[matchIndex - rf.startIndex].Term {
				break
			}
		}
		if matchIndex == rf.startIndex {
			matchIndex = rf.log[0].Index
		}
		return false, matchIndex
	}
	return true, -1
}

func electionTimeOut() time.Duration {
	// Set election timer to a randomized value between 150ms and 200ms.
	return time.Duration(150 + rand.Intn(50)) * time.Millisecond
}

func (rf *Raft) followerLoop() {
	rf.mu.Lock()
	rf.role = RoleFollower
	rf.mu.Unlock()

	timer := time.NewTimer(electionTimeOut())

	for {
		select {
			case <-rf.killCh: {
				return
			}
			case heartBeat := <-rf.heartBeatCh: {
				rf.mu.Lock()
				// Received a heart beat from a leader.
				rf.DPrintf("Processing heartbeat from leader %d with term %d, my term is %d.\n",
					heartBeat.leaderId, heartBeat.term, rf.currentTerm)
				if heartBeat.term < rf.currentTerm {
					// The leader is a stale leader, ignore and let the next time's firing handles it.
					rf.DPrintf("Heartbeat is from a stale leader, ignore.\n")
				} else {
					rf.setCurrentTerm(heartBeat.term)
					rf.persist()
					timer.Reset(electionTimeOut())
				}
				rf.mu.Unlock()
			}
			case <-timer.C: {
				// Leader is probably down, restart a new leader election.
				rf.DPrintf("Converting to candidate for leader election.")
				go rf.candidateLoop()
				return
			}
		}
	}
}

func (rf *Raft) candidateLoop() {
	rf.mu.Lock()
	rf.role = RoleCandidate
	rf.mu.Unlock()

	for {
		// Increase the current term and vote for itself.
		rf.mu.Lock()
		rf.setCurrentTerm(rf.currentTerm + 1)
		rf.voteFor = rf.me
		rf.persist()
		rf.mu.Unlock()

		replies := make(chan RequestVoteReply)
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(i int) {
				var args RequestVoteArgs
				var reply RequestVoteReply

				rf.mu.Lock()
				lastLogIndex, lastLogTerm := rf.lastLogIndex()
				args.LastLogIndex = lastLogIndex
				args.LastLogTerm = lastLogTerm
				args.CandidateId = rf.me
				args.Term = rf.currentTerm
				rf.mu.Unlock()
				if rf.sendRequestVote(i, args, &reply) {
					replies <- reply
				}
			}(i)
		}

		votes := 1

L:
		for {
			select {
				case <-rf.killCh: {
					return
				}
				case <- rf.downRoleCh: {
					rf.DPrintf("Converting to follower because of downRoleCh.\n")
					go rf.followerLoop()
					return
				}
				case reply := <-replies: {
					rf.mu.Lock()
					exit := false
					if rf.checkStaleLeader("CandidateLoop", reply.Term, reply.FollowerId) {
						rf.persist()
						go rf.followerLoop()
						exit = true
					} else if reply.VoteGranted {
						rf.DPrintf("Received a vote from %d.\n", reply.FollowerId)
						votes += 1
						if (votes > len(rf.peers) / 2) {
							// Donald J. Trump wins his election!
							rf.DPrintf("Won election, converting to leader. CommitIndex = %d.\n", rf.commitIndex)
							go rf.leaderLoop()
							exit = true
						}
					}
					rf.mu.Unlock()
					if exit {
						return
					}
				}
				case <- time.After(electionTimeOut()): {
					// Probably a split vote or out of majority, no leader is elected, retry another round.
					rf.DPrintf("Probably a split vote or out of majority, retry another round of election.\n")
					break L
				}
			}
		}
	}
}

// Invoked by a leader to synchronize logs to followers.
// The synchronization process should be ran in a separate go routine.
func (rf *Raft) leaderSyncLogs(server int) {
	for {
		rf.mu.Lock()

		lostLeadership := rf.role != RoleLeader
		lastLogIndex, _ := rf.lastLogIndex()

		sendSnapshot := false

		// Send snapshot if the log to be replicated is discarded.
		if rf.nextIndex[server] <= rf.startIndex {
			sendSnapshot = true
		}

		var argsAppend AppendEntriesArgs
		var replyAppend AppendEntriesReply
		var argsSnapshot InstallSnapshotArgs
		var replySnapshot InstallSnapshotReply

		// Check leadership here as nextIndex may not be valid if lose leadership.
		if !lostLeadership {
			if sendSnapshot {
				argsSnapshot.LeaderId = rf.me
				argsSnapshot.Term = rf.currentTerm
				argsSnapshot.LastIncludedIndex = rf.log[0].Index
				argsSnapshot.LastIncludedTerm = rf.log[0].Term
				argsSnapshot.Snapshot = rf.persister.ReadSnapshot()
			} else {
				argsAppend.IsHeartBeat = false
				argsAppend.LeaderCommit = rf.commitIndex
				argsAppend.LeaderId = rf.me
				argsAppend.Term = rf.currentTerm
				argsAppend.PrevLogIndex = rf.nextIndex[server] - 1
				argsAppend.PrevLogTerm = rf.log[rf.nextIndex[server] - 1 - rf.startIndex].Term
				for j := argsAppend.PrevLogIndex + 1; j <= lastLogIndex; j++ {
					argsAppend.Entries = append(argsAppend.Entries, rf.log[j - rf.startIndex])
				}
			}
		}

		rf.mu.Unlock()

		if lostLeadership {
			return
		}

		var repliedTerm int

		if sendSnapshot {
			rf.mu.Lock()
			rf.DPrintf("Synchronizing log for server %d by InstallSnapshot. nextIndex[%d] = %d, startIndex = %d.\n",
				server, server, rf.nextIndex[server], rf.startIndex)
			rf.mu.Unlock()
			if !rf.sendInstallSnapshot(server, argsSnapshot, &replySnapshot) {
				return
			}
			repliedTerm = replySnapshot.Term
		} else {
			rf.DPrintf("Synchronizing log for server %d by AppendEntries.\n", server)
			if !rf.sendAppendEntries(server, argsAppend, &replyAppend) {
				return
			}
			repliedTerm = replyAppend.Term
		}

		rf.mu.Lock()

		if rf.checkStaleLeader("SyncLog", repliedTerm, server) {
			rf.persist()
			rf.mu.Unlock()
			return
		}

		// Update nextIndex and matchIndex for server.
		setLastIndex := func(index int) {
			if rf.nextIndex[server] < index + 1 {
				rf.nextIndex[server] = index + 1
			}
			if rf.matchIndex[server] < index {
				rf.matchIndex[server] = index
			}
		}

		if sendSnapshot {
			if replySnapshot.Success {
				setLastIndex(argsSnapshot.LastIncludedIndex)
				rf.DPrintf("Snapshot replicated to server %d nextIndex = %d, matchIndex = %d.\n",
					server, rf.nextIndex[server], rf.matchIndex[server])
				rf.mu.Unlock()
				return
			}
		} else {
			if replyAppend.Success {
				setLastIndex(lastLogIndex)
				if len(argsAppend.Entries) != 0 {
					rf.DPrintf("Log replicated to server %d nextIndex = %d, matchIndex = %d.\n",
						server, rf.nextIndex[server], rf.matchIndex[server])
				}
				rf.mu.Unlock()
				return
			} else if replyAppend.NextIndex != -1 {
				// Rejected because of inconsistent logs.
				rf.nextIndex[server] = replyAppend.NextIndex + 1
				rf.DPrintf("Failed to AppendEntries for server %d, retry with decreasing nextIndex to %d.\n",
					server, rf.nextIndex[server])
			}
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) leaderLoop() {
	rf.mu.Lock()

	rf.role = RoleLeader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	lastLogIndex, _ := rf.lastLogIndex()
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = lastLogIndex + 1
	}

	rf.mu.Unlock()

	// Try AppendEntries asynchronously if followers miss some.
	for i := range rf.peers {
		if i != rf.me {
			go rf.leaderSyncLogs(i)
		}
	}

	for {
		// Update commitIndex first.
		rf.mu.Lock()
		newCommitIndex := rf.commitIndex
		for c := rf.startIndex + len(rf.log) - 1; c > rf.commitIndex; c-- {
			if rf.log[c - rf.startIndex].Term == rf.currentTerm {
				count := 1
				for i := range rf.peers {
					if i == rf.me {
						continue
					}
					if rf.matchIndex[i] >= c {
						count += 1
					}
				}
				if count > len(rf.peers) / 2 {
					newCommitIndex = c
					break
				}
			}
		}
		if newCommitIndex != rf.commitIndex {
			// Log is replicated to majority, commit log on leader.
			rf.DPrintf("Increasing leader's commit index from %d to %d.\n", rf.commitIndex, newCommitIndex)

			for i := rf.commitIndex + 1; i <= newCommitIndex; i++ {
				var applyMsg ApplyMsg

				applyMsg.Index = i
				applyMsg.Term = rf.log[i - rf.startIndex].Term
				applyMsg.Command = rf.log[i - rf.startIndex].Command
				applyMsg.UseSnapshot = false
				rf.applyCh <- applyMsg
			}
			rf.commitIndex = newCommitIndex

			// Also let each follower commit.
			for i := range rf.peers {
				if i != rf.me {
					go rf.leaderSyncLogs(i)
				}
			}
		}
		rf.mu.Unlock()

		// Do heartbeat broadcasting.
		replies := make(chan AppendEntriesReply)
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(i int) {
				var args AppendEntriesArgs
				var reply AppendEntriesReply

				rf.mu.Lock()

				lostLeadership := rf.role != RoleLeader
				missingLog := rf.nextIndex[i] < rf.startIndex + len(rf.log)

				// Check leadership here as nextIndex may not be valid if lose leadership.
				if !lostLeadership {
					args.IsHeartBeat = true
					args.LeaderCommit = rf.commitIndex
					args.LeaderId = rf.me
					args.Term = rf.currentTerm

					if !missingLog {
						args.PrevLogIndex = rf.nextIndex[i] - 1
						args.PrevLogTerm = rf.log[rf.nextIndex[i] - 1 - rf.startIndex].Term
					}
				}

				rf.mu.Unlock()

				if lostLeadership {
					return
				}
				// Synchronize logs if inconsistent or missing.
				if missingLog || (rf.sendAppendEntries(i, args, &reply) && !reply.Success) {
					go rf.leaderSyncLogs(i)
					if !missingLog {
						replies <- reply
					}
				}
			}(i)
		}

L:
		for {
			select {
				case <-rf.killCh: {
					return
				}
				case <-rf.downRoleCh: {
					rf.DPrintf("Converting to follower because of downRoleCh.\n")
					go rf.followerLoop()
					return
				}
				case <-time.After(time.Duration(60) * time.Millisecond): {
					// We do heartbeat broadcasting every 60ms. Now time is up, start another round of broadcast.
					break L
				}
				case reply := <-replies: {
					rf.mu.Lock()
					exit := false
					if rf.checkStaleLeader("Heartbeat", reply.Term, reply.FollowerId) {
						rf.persist()
						go rf.followerLoop()
						exit = true
					} else {
						rf.DPrintf("Heart beat broadcast receives ACK from %d.\n", reply.FollowerId)
					}
					rf.mu.Unlock()
					if exit {
						return
					}
				}
			}
		}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != RoleLeader {
		return -1, -1, false
	}

	// We think we are leader.
	rf.DPrintf("Starting agreement on command %v.\n", command)

	logIndex, _ := rf.lastLogIndex()
	logIndex += 1

	var log Log
	log.Index = logIndex
	log.Term = rf.currentTerm
	log.Command = command

	rf.log = append(rf.log, log)
	rf.persist()

	for i := range rf.peers {
		if i != rf.me {
			go rf.leaderSyncLogs(i)
		}
	}

	return logIndex, log.Term, true
}

//
// the tester calls Kill() when a Raft instance won't be needed again.
//
func (rf *Raft) Kill() {
	rf.killCh <- struct{}{}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.log = make([]Log, 1)
	rf.log[0].Index = 0
	rf.log[0].Term = 0

	rf.commitIndex = 0
	rf.startIndex = 0

	rf.role = RoleFollower
	rf.currentTerm = 0
	rf.voteFor = -1

	rf.killCh = make(chan struct{})
	rf.downRoleCh = make(chan struct{})
	rf.heartBeatCh = make(chan HeartBeat)
	rf.applyCh = applyCh

	// Initialize from state persisted before a crash.
	rf.readPersist(persister.ReadRaftState())

	rf.DPrintf("Server is up, commitIndex = %d.\n", rf.commitIndex)

	// Start as follower.
	go rf.followerLoop()

	return rf
}
