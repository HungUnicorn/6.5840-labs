package shardgrp

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)

type Clerk struct {
	*tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	mu      sync.Mutex
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{Clnt: clnt, servers: servers}
	return ck
}

func (ck *Clerk) Leader() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	return ck.leader
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}
	for {
		ck.mu.Lock()
		leader := ck.leader
		serverName := ck.servers[leader]
		ck.mu.Unlock()

		var reply rpc.GetReply
		ok := ck.Clnt.Call(serverName, "KVServer.Get", &args, &reply)
		if ok {
			if reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey || reply.Err == rpc.ErrWrongGroup {
				return reply.Value, reply.Version, reply.Err
			}
		}

		ck.mu.Lock()
		ck.leader = (ck.leader + 1) % len(ck.servers)
		ck.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}

	firstAttempt := true
	for {
		ck.mu.Lock()
		leader := ck.leader
		serverName := ck.servers[leader]
		ck.mu.Unlock()

		var reply rpc.PutReply
		ok := ck.Clnt.Call(serverName, "KVServer.Put", &args, &reply)
		if ok {
			if reply.Err == rpc.OK {
				return rpc.OK
			}
			if reply.Err == rpc.ErrVersion || reply.Err == rpc.ErrNoKey || reply.Err == rpc.ErrWrongGroup {
				if firstAttempt {
					return reply.Err
				}
				return rpc.ErrMaybe
			}
		}

		firstAttempt = false

		ck.mu.Lock()
		ck.leader = (ck.leader + 1) % len(ck.servers)
		ck.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	args := shardrpc.FreezeShardArgs{Shard: s, Num: num}
	for {
		ck.mu.Lock()
		leader := ck.leader
		serverName := ck.servers[leader]
		ck.mu.Unlock()

		var reply shardrpc.FreezeShardReply
		ok := ck.Clnt.Call(serverName, "KVServer.FreezeShard", &args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader && reply.Err != "" {
			return reply.State, reply.Err
		}

		ck.mu.Lock()
		ck.leader = (ck.leader + 1) % len(ck.servers)
		ck.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.InstallShardArgs{Shard: s, State: state, Num: num}
	for {
		ck.mu.Lock()
		leader := ck.leader
		serverName := ck.servers[leader]
		ck.mu.Unlock()

		var reply shardrpc.InstallShardReply
		ok := ck.Clnt.Call(serverName, "KVServer.InstallShard", &args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader && reply.Err != "" {
			return reply.Err
		}

		ck.mu.Lock()
		ck.leader = (ck.leader + 1) % len(ck.servers)
		ck.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.DeleteShardArgs{Shard: s, Num: num}
	for {
		ck.mu.Lock()
		leader := ck.leader
		serverName := ck.servers[leader]
		ck.mu.Unlock()

		var reply shardrpc.DeleteShardReply
		ok := ck.Clnt.Call(serverName, "KVServer.DeleteShard", &args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader && reply.Err != "" {
			return reply.Err
		}

		ck.mu.Lock()
		ck.leader = (ck.leader + 1) % len(ck.servers)
		ck.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}
}
