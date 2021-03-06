# kvraft-golang

Fault-tolerant Key/Value service based on Raft consensus algorithm.

This code is derived from [MIT 6.824](https://pdos.csail.mit.edu/6.824/schedule.html) Spring 2016's Lab. All tests under `raft/` and `kvraft/` are passed.

Features implemented so far:

* Leader election
* Log replication
* Snapshot and log compaction
* Persistent for recovery from crash
* Key/Value server built on top of Raft

WIP:

* Shard Key/Value service to improve performance (shard master is done)

# Raft notes

See `Raft.md`.

# Credits

* [MIT 6.824 Distributed Systems](https://pdos.csail.mit.edu/6.824/schedule.html) 
* [Raft paper](https://raft.github.io/raft.pdf), O Diego
* [runshenzhu](https://github.com/runshenzhu/RaftGo/)'s implementation. A great reference for me to get started with how a bare Raft is implemented!
* [vizee](https://github.com/vizee). Golang elder driver carrys me flying by teaching me some basics and traps of golang.
