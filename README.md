# MIT 6.5840: Distributed Systems (Spring 2026)

This repository contains my personal implementations for the labs in [MIT's Graduate Distributed Systems course (6.5840)](https://pdos.csail.mit.edu/6.824/schedule.html). Each lab focuses on building a different component of a robust, scalable, and fault-tolerant distributed system.

## 📊 Progress Tracker

| Lab | Title                                                          | Status | Key Concepts |
| :--- |:---------------------------------------------------------------| :--- | :--- |
| **1** | [MapReduce](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html) | ✅ Complete | Fault tolerance, RPCs, Data Partitioning |
| **2** | [Key/Value Server](https://pdos.csail.mit.edu/6.824/labs/lab-kvsrv1.html) | ✅ Complete | At-Most-Once, Ambiguity Resolution, Locking |

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
---

## 🧪 Testing

The labs are verified using the official MIT test suite. To deepen understanding and ensure robustness, I have added some unit tests.

## 🏗️ Future Labs (Placeholders)

| Lab | Title | Status |
| :--- | :--- | :--- |
| **3** | Raft Consensus | ⏳ Pending |