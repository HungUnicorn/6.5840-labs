package shardgrp

import (

	"bytes"
	"log"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)

const (
	ENVKEY = "65840ENV"
)


type ValueVersion struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me  int
	rsm *rsm.RSM
	gid tester.Tgid

	mu          sync.Mutex
	db          map[string]ValueVersion
	ownedShards map[shardcfg.Tshid]bool
	shardNum    map[shardcfg.Tshid]shardcfg.Tnum
}


func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch args := req.(type) {
	case rpc.GetArgs:
		var reply rpc.GetReply
		shard := shardcfg.Key2Shard(args.Key)
		if !kv.ownedShards[shard] {
			reply.Err = rpc.ErrWrongGroup
			return reply
		}

		valVer, exists := kv.db[args.Key]
		if exists {
			reply.Value = valVer.Value
			reply.Version = valVer.Version
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
		return reply

	case rpc.PutArgs:
		var reply rpc.PutReply
		shard := shardcfg.Key2Shard(args.Key)
		if !kv.ownedShards[shard] {
			reply.Err = rpc.ErrWrongGroup
			return reply
		}

		valVer, exists := kv.db[args.Key]
		if !exists {
			if args.Version != 0 {
				reply.Err = rpc.ErrNoKey
			} else {
				kv.db[args.Key] = ValueVersion{
					Value:   args.Value,
					Version: 1,
				}
				reply.Err = rpc.OK
			}
		} else {
			if args.Version != valVer.Version {
				reply.Err = rpc.ErrVersion
			} else {
				kv.db[args.Key] = ValueVersion{
					Value:   args.Value,
					Version: valVer.Version + 1,
				}
				reply.Err = rpc.OK
			}
		}
		return reply
	
	case shardrpc.FreezeShardArgs:
		var reply shardrpc.FreezeShardReply
		if args.Num < kv.shardNum[args.Shard] {
			reply.Err = rpc.OK
			return reply
		}
		kv.shardNum[args.Shard] = args.Num

		state := make(map[string]ValueVersion)
		for k, v := range kv.db {
			if shardcfg.Key2Shard(k) == args.Shard {
				state[k] = v
			}
		}

		delete(kv.ownedShards, args.Shard)

		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(state)
		reply.State = w.Bytes()
		reply.Err = rpc.OK
		return reply

	case shardrpc.InstallShardArgs:
		var reply shardrpc.InstallShardReply
		if args.Num < kv.shardNum[args.Shard] {
			reply.Err = rpc.OK
			return reply
		}
		kv.shardNum[args.Shard] = args.Num

		r := bytes.NewBuffer(args.State)
		d := labgob.NewDecoder(r)
		var state map[string]ValueVersion
		d.Decode(&state)

		for k, v := range state {
			kv.db[k] = v
		}

		kv.ownedShards[args.Shard] = true
		reply.Err = rpc.OK
		return reply

	case shardrpc.DeleteShardArgs:
		var reply shardrpc.DeleteShardReply
		if args.Num < kv.shardNum[args.Shard] {
			reply.Err = rpc.OK
			return reply
		}
		kv.shardNum[args.Shard] = args.Num

		for k := range kv.db {
			if shardcfg.Key2Shard(k) == args.Shard {
				delete(kv.db, k)
			}
		}
		delete(kv.ownedShards, args.Shard)

		reply.Err = rpc.OK
		return reply
	}

	return nil
}

func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(kv.db); err != nil {
		log.Fatalf("KVServer: failed to encode db: %v", err)
	}
	if err := e.Encode(kv.ownedShards); err != nil {
		log.Fatalf("KVServer: failed to encode ownedShards: %v", err)
	}
	if err := e.Encode(kv.shardNum); err != nil {
		log.Fatalf("KVServer: failed to encode shardNum: %v", err)
	}
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	if len(data) == 0 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var db map[string]ValueVersion
	var ownedShards map[shardcfg.Tshid]bool
	var shardNum map[shardcfg.Tshid]shardcfg.Tnum
	if err := d.Decode(&db); err != nil {
		log.Fatalf("KVServer: failed to decode db: %v", err)
	}
	if err := d.Decode(&ownedShards); err != nil {
		log.Fatalf("KVServer: failed to decode ownedShards: %v", err)
	}
	if err := d.Decode(&shardNum); err != nil {
		log.Fatalf("KVServer: failed to decode shardNum: %v", err)
	}

	kv.mu.Lock()
	kv.db = db
	kv.ownedShards = ownedShards
	kv.shardNum = shardNum
	kv.mu.Unlock()
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(rpc.PutReply)
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(shardrpc.FreezeShardReply)
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(shardrpc.InstallShardReply)
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(shardrpc.DeleteShardReply)
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{gid: gid, me: me}
	kv.db = make(map[string]ValueVersion)
	kv.ownedShards = make(map[shardcfg.Tshid]bool)
	kv.shardNum = make(map[shardcfg.Tshid]shardcfg.Tnum)
	if gid == shardcfg.Gid1 {
		for i := shardcfg.Tshid(0); i < shardcfg.NShards; i++ {
			kv.ownedShards[i] = true
		}
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartServerShardGrp(ends, grp, srv, persister, tester.MaxRaftState)
}
