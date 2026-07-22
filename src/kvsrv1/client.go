package kvsrv

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)

const retryInterval = 100 * time.Millisecond

type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	return ck
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}

	for {
		reply := rpc.GetReply{}
		ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)

		if ok {
			return reply.Value, reply.Version, reply.Err
		}

		time.Sleep(retryInterval)
	}
}

func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}
	rpcMayHaveBeenDelivered := false

	for {
		reply := rpc.PutReply{}
		ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)

		if ok {
			if reply.Err == rpc.ErrVersion && rpcMayHaveBeenDelivered {
				return rpc.ErrMaybe
			}
			return reply.Err
		}

		rpcMayHaveBeenDelivered = true

		time.Sleep(retryInterval)
	}
}
