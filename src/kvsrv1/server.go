package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ValueVersion struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex
	db map[string]ValueVersion
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		db: make(map[string]ValueVersion),
	}
	return kv
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if valVer, exists := kv.db[args.Key]; exists {
		reply.Value = valVer.Value
		reply.Version = valVer.Version
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Err = kv.applyPut(args.Key, args.Value, args.Version)
}

func (kv *KVServer) applyPut(key, value string, reqVersion rpc.Tversion) rpc.Err {
	valVer, exists := kv.db[key]

	if !exists {
		return kv.insertNewKey(key, value, reqVersion)
	}
	return kv.updateExistingKey(key, value, reqVersion, valVer.Version)
}

func (kv *KVServer) insertNewKey(key, value string, reqVersion rpc.Tversion) rpc.Err {
	if reqVersion != 0 {
		return rpc.ErrNoKey
	}

	kv.db[key] = ValueVersion{
		Value:   value,
		Version: 1,
	}
	return rpc.OK
}

func (kv *KVServer) updateExistingKey(key, value string, reqVersion, currentVersion rpc.Tversion) rpc.Err {
	if reqVersion != currentVersion {
		return rpc.ErrVersion
	}

	kv.db[key] = ValueVersion{
		Value:   value,
		Version: currentVersion + 1,
	}
	return rpc.OK
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
