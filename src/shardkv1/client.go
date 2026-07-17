package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"sync"

	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardctrler"
	"6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	rcks map[tester.Tgid]*shardgrp.Clerk
	mu   sync.Mutex
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
	}
	ck.rcks = make(map[tester.Tgid]*shardgrp.Clerk)
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) GetClerk(gid tester.Tgid) (*shardgrp.Clerk, bool) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	rck, ok := ck.rcks[gid]
	return rck, ok
}


// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	shard := shardcfg.Key2Shard(key)
	cfg := ck.sck.Query()
	if cfg == nil {
		return "", 0, rpc.ErrMaybe
	}
	gid, servers, ok := cfg.GidServers(shard)
	if !ok {
		return "", 0, rpc.ErrMaybe
	}

	rck, ok := ck.GetClerk(gid)
	if !ok {
		rck = shardgrp.MakeClerk(ck.clnt, servers)
		ck.mu.Lock()
		ck.rcks[gid] = rck
		ck.mu.Unlock()
	}

	return rck.Get(key)
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	shard := shardcfg.Key2Shard(key)
	cfg := ck.sck.Query()
	if cfg == nil {
		return rpc.ErrMaybe
	}
	gid, servers, ok := cfg.GidServers(shard)
	if !ok {
		return rpc.ErrMaybe
	}

	rck, ok := ck.GetClerk(gid)
	if !ok {
		rck = shardgrp.MakeClerk(ck.clnt, servers)
		ck.mu.Lock()
		ck.rcks[gid] = rck
		ck.mu.Unlock()
	}

	return rck.Put(key, value, version)
}
