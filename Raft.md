# Raft

## Introduction (taken from Raft Lab note)

A replicated service (e.g., key/value database) uses Raft to help manage its replica servers. The point of having replicas is so that the service can continue operating even if some of the replicas experience failures (crashes or a broken or flaky network). The challenge is that, due to these failures, the replicas won't always hold identical data; Raft helps the service sort out what the correct data is.

Raft's basic approach is to implement a *replicated state machine*. Raft organizes client requests into a sequence, called the *log*, and ensures that all the replicas agree on the the contents of the log. Each replica executes the client requests in the log in the order they appear in the log, applying those requests to the service's state. Since all the live replicas see the same log contents, they all execute the same requests in the same order, and thus continue to have identical service state. If a server fails but later recovers, Raft takes care of bringing its log up to date. Raft will continue to operate as long as at least a majority of the servers are alive and can talk to each other. If there is no such majority, Raft will make no progress, but will pick up where it left off as soon as a majority is alive again. 

## Key points of Raft

* Raft modularizes leader election, log replication and safety.

* Raft uses terminology *term* to represent logical time. Each term can be roughly seen as some kind of peaceful period, i.e. no leader dies, no follower loses contact due to flaky network. Terms allow servers to detect obsolete information such as stale leaders. 

* Raft gets its power by two kind of RPCS, RequestVote and AppendEntries. Heartbeat is a special kind RPC of AppendEntries with empty logs.

* A cluster of servers can have three states, follower, candidate and leader. At most one server can be leader in a term. Leader keeps its identity by broadcasting heartbeat packets. If a follower observes no heartbeat from a leader for a while, it converts to candidate and advances to a new term to start an election via RequestVote RPC (a randomized election timeout is used to avoid multiple candidates requesting for vote). If it gains majority's vote, converts to leader, otherwise restart a new election.

* Terms will get exchanged when servers communicate, leader will drop out if it finds its term is stale.

* Log entries can only flow from leader to slaves. When client is appending a new log entry into leader's logs, it will trigger leader to synchronizing (or replicating) logs to slaves. Leader will try to replicate *all* logs till now to slaves at each synchronization. After one log is replicated to the majority of servers, leader can start committing this log, so this log will be applied to the state replication machine (both are done in AppendEntries RPC). This implies that a winning candidate in an election should contain all committed entries. To achieve this, Raft only elects leader whose logs are newer than majority.

* Log committing is controlled by an variable `commitIndex` in leader. Leader thinks we can advance `commitIndex` if terms are equal and logs are replicated to majority. From a single log entry perspective, a log entry becomes commited if:

  1) it reached a majority in the term it was initially sent out, or

  2) if a subsequent log entry becomes committed.

* When leader is appending entries, it includes the index and term of the entry in its log that immediately precedes the new entries. Slaves may refuse to append new log entries if it doesn't find the log in entries, and let leader retry with log entries in the conflicting term to override the inconsistent ones. Overriding them is safe as they are not committed yet.

* Raft can recover from crashing at any time point. More precisely, Raft should persist `logs`, `curTerm` and `voteFor` to avoid voting for two candidates at one term. Persisting should happen every time after these volatile variables are changed. Logs compaction must always happen after snapshot is saved on disk. We require the disk storage system has some features like GFS's atomic rename.

* RPCs may come out of order. Implementation should handle a stale RPC request correctly, for example, not overwriting newly replicated logs by stale logs from a slow RPC.

* Raft maintains its correctness by keeps these five principles as invariant.

  1) Election Safety: at most one leader can be elected in a given term.

  2) Leader Append-Only: a leader never overwrites or deletes entries in its log; it only appends new entries.

  3) Log Matching: if two logs contain an entry with the same index and term, then the logs are identical in all entries up through the given index.

  4) Leader Completeness: if a log entry is committed in a given term, then that entry will be present in the logs of the leaders for all higher-numbered terms.

  5) State Machine Safety: if a server has applied a log entry at a given index to its state machine, no other server will ever apply a different log entry for the same index.

## Notes on RPC

RPC is built on top of TCP/IP protocol, no re-ordering but may drop message. Dropping message between Raft servers is okay, but goes a little different when we are considering key/value servers built on top of Raft.

The key/value client is not running concurrently (only one thread/goroutine talking to server), this means the serial identifier of client's request is appearing in the increasing order, but not strictly increasing because network may drop message. When the client gets time-out, it will re-send a request with the same serial identifier. The problem is how could key/value server handle duplicate requests?

        client                 server
        PUT 1            -->
                         <--   (network drops it) PUT 1 OK
        PUT 1 timeout
        PUT 1            -->
                               PUT 1 (a duplicate request, re-execute or ignore?)

We choose the at-most-once semantic to avoid duplicate requests.

* Client's unique identifier.

* Request's strictly increasing serial identifier.

* Server caches last request's execute result.

* (if re-ordering exists) or server maintains a window of executed results, and client includes the last continuous identifier of received result from server, so the server can shrink the window to fit in memory.
