# MIT 6.5840: Distributed Systems (Spring 2026)

This repository contains my personal implementations for the labs in [MIT's Graduate Distributed Systems course (6.5840)](https://pdos.csail.mit.edu/6.824/schedule.html). Each lab focuses on building a different component of a robust, scalable, and fault-tolerant distributed system.

## 📊 Progress Tracker

| Lab | Title                                                          | Status | Key Concepts                         |
| :--- |:---------------------------------------------------------------| :--- |:-------------------------------------|
| **1** | [MapReduce](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html) | ✅ Complete | Fault tolerance, RPCs, Data Partitioning |
| **2** | [Key/Value Server](https://pdos.csail.mit.edu/6.824/labs/lab-kvsrv1.html) | ✅ Complete | At-Most-Once, Ambiguity Resolution, Locking |
| **3** | [Raft Consensus](https://pdos.csail.mit.edu/6.824/labs/lab-raft1.html) | ✅ Complete | Leader Election, Log Replication, Persistence, Snapshots |
| **4** | [Fault-tolerant Key/Value Service](https://pdos.csail.mit.edu/6.824/labs/lab-kvraft1.html) | ✅ Complete | Replicated State Machine (RSM), Caching, Ambiguity Resolution, Snapshots |
| **5** | [Sharded Key/Value Service](https://pdos.csail.mit.edu/6.824/labs/lab-shard.html) | 🟡 In Progress | Sharding, Configuration Management, Data Migration |
---

## Lab 1: MapReduce

A distributed MapReduce system built in Go, modeled after the original Google architecture. It utilizes a central Coordinator to manage tasks and stateless Workers to process data in parallel. Unit tests are added to `coordinator_tests.go` and `worker_tests.go` to help better understanding.

### Core Features
* **Fault Tolerance:** A 10-second watchdog timer in the Coordinator detects and reassigns tasks from crashed or slow workers.
* **Atomic Commits:** Workers use a "write-to-temp-and-rename" pattern to ensure that partial failures never result in corrupted output files.
* **Deterministic Partitioning:** Uses `ihash` to ensure all instances of the same key map to the same Reduce bucket.

---

## Lab 2: Key/Value Server & Distributed Lock

A fault-tolerant Key/Value service and a distributed lock implementation designed to operate over an unreliable network.

### Key Components
* **Ambiguity Resolution:** When a network drop occurs, the Clerk identifies "maybe" scenarios where a request might have succeeded on the server but the acknowledgment was lost.
* **Distributed Lock:** A robust lock implementation using unique Owner IDs. It resolves state ambiguity by re-verifying the key value after an `ErrMaybe` response.

## Lab 3: Raft Consensus

A Go implementation of the **Raft distributed consensus protocol**, building a replicated state machine that remains consistent despite network partitions and server failures.

### Part 3A: Leader Election & Heartbeats
The first phase implements the core election sub-problem and leadership authority maintenance.

* **State Machine Transitions:** Precise implementation of **Follower**, **Candidate**, and **Leader** roles.
* **Heartbeat Mechanism:** Established a background `heartbeatTicker` to broadcast `AppendEntries` RPCs, suppressing subordinate elections and asserting authority.

### Part 3B: Log Replication
This phase focuses on the core consensus mechanism, ensuring that logs are consistently replicated across a majority of followers before being committed.

* **Log Consistency:** Strict conflict optimization and fast log rollback logic in `AppendEntries` to quickly resolve divergences.
* **Commit Progression:** The leader accurately updates its commit index based on the highest log index replicated to a majority of peers.

### Part 3C: Persistence
Servers must be able to recover from crashes and resume their place in the cluster without jeopardizing safety.

* **Durable State:** Raft systematically saves `currentTerm`, `votedFor`, and `logEntries` to non-volatile storage (simulated via `tester1.Persister`) using the `labgob` encoder before responding to RPCs.
* **Crash Recovery:** Servers seamlessly rebuild their state from persistence upon initialization.

### Part 3D: Log Compaction (Snapshots)
A long-running service cannot retain its entire log in memory forever. Raft coordinates with the state machine to truncate old log entries using snapshots.

* **Offset Architecture:** Decoupled the cluster's logical indices from the Go slice physical indices using a `snapshotIndex` offset mapping (`logical2Physical`).
* **InstallSnapshot RPC:** The leader automatically identifies when a follower has fallen so far behind that the necessary logs have already been truncated, forcing synchronization via an `InstallSnapshot` RPC.

---

## Lab 4: Fault-tolerant Key/Value Service

A fault-tolerant key/value storage service built on top of the Raft consensus engine.

### Part 4A: Replicated State Machine (RSM)
Decoupled the consensus layer from the application logic by implementing a generic Replicated State Machine wrapper.

* **State Machine Interface**: Implemented the `DoOp` abstraction, allowing any deterministic state machine (like a simple counter or a KV store) to run over Raft.
* **Concurrent Submission**: Used client-specific operation tracking and index mapping to manage multiple concurrent client requests safely.
* **Reactive Aborts**: Client requests quickly fail with `ErrWrongLeader` on network partitions or term changes, prompting immediate leader rediscovery.

### Part 4B: Key/Value Service without Snapshots
Built a replicated, linearizable Key/Value database server cluster (`KVServer`) and a client Clerk on top of the RSM layer.

* **Replicated DB Transitions**: Implemented versioned state transitions in `DoOp()`, allowing updates to replicate and execute consistently on all database copies.
* **Clerk Leadership Caching**: Designed client routing with a cached leader pointer to route requests directly, falling back to sequential leader discovery only during failures.
* **At-Most-Once Ambiguity Handling**: Engineered retransmission rules that identify lost network acknowledgments and report `ErrMaybe` to prevent duplicate executions.

### Part 4C: Key/Value Service with Snapshots
Enabled persistent log compaction using serialized state snapshots, preventing infinite log growth and enabling rapid node recovery.

* **Durable Snapshots**: Implemented `Snapshot()` and `Restore()` in the database layer to serialize state into bytes for Raft storage.
* **Auto-compaction**: Programmed the RSM to monitor log size (`rf.PersistBytes()`) and trigger snapshots automatically when it exceeds the state threshold.
* **Ingest & Install Handling**: Handled incoming snapshot installations from the leader to bring disconnected or slow followers up to date instantly.

---

## Lab 5: Sharded Key/Value Service

A scalable sharded key/value database where the keyspace is partitioned across multiple Raft replica groups to increase throughput and storage capacity.

### Part 5A: The Shard Controller and Static Sharding
Implemented the central coordination system to manage the assignment of shards to replica groups.

* **Shard Controller Orchestration:** Built the `shardctrler` using a Raft-backed kvsrv to maintain a strictly linearizable sequence of configuration changes. When new server groups join or leave, it automatically executes a greedy load-balancing algorithm to seamlessly reassign shards.
* **Deterministic Routing & Retries:** Programmed the client to deterministically route keys to specific server groups based on the active `ShardConfig`. Implemented transparent retry logic when encountering `ErrWrongGroup` to handle requests sent during a configuration shift.
* **Live State Migration:** Orchestrated the movement of live data across server groups using `FreezeShard`, `InstallShard`, and `DeleteShard` RPCs. This strictly ensures no operations are processed on stale data during the transition, maintaining perfect linearizability.

---

## 🧪 Testing

The labs are verified using the official MIT test suite. To deepen understanding and ensure robustness, I have added some unit tests.
