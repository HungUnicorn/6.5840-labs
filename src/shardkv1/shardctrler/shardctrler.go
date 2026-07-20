package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (

	"6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	"6.5840/tester1"
)


// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

func (sck *ShardCtrler) moveShards(oldConfig, newConfig *shardcfg.ShardConfig) {
	for shardID := 0; shardID < shardcfg.NShards; shardID++ {
		oldGroup := oldConfig.Shards[shardID]
		newGroup := newConfig.Shards[shardID]

		isSameGroup := oldGroup == newGroup
		isUnassignedOld := oldGroup == 0
		isUnassignedNew := newGroup == 0

		if isSameGroup || isUnassignedOld || isUnassignedNew {
			continue
		}

		oldClient := shardgrp.MakeClerk(sck.clnt, oldConfig.Groups[oldGroup])
		newClient := shardgrp.MakeClerk(sck.clnt, newConfig.Groups[newGroup])

		state, err := oldClient.FreezeShard(shardcfg.Tshid(shardID), newConfig.Num)
		if err != rpc.OK {
			continue
		}

		newClient.InstallShard(shardcfg.Tshid(shardID), state, newConfig.Num)
		oldClient.DeleteShard(shardcfg.Tshid(shardID), newConfig.Num)
	}
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	nextConfigStr, _, _ := sck.IKVClerk.Get("next_config")
	if nextConfigStr == "" {
		return
	}
	nextConfig := shardcfg.FromString(nextConfigStr)

	currentConfigStr, currentConfigVer, _ := sck.IKVClerk.Get("config")
	if currentConfigStr == "" {
		return
	}
	currentConfig := shardcfg.FromString(currentConfigStr)

	if nextConfig.Num <= currentConfig.Num {
		return
	}

	sck.moveShards(currentConfig, nextConfig)
	sck.IKVClerk.Put("config", nextConfig.String(), currentConfigVer)
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	sck.IKVClerk.Put("config", cfg.String(), 0)
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(newConfig *shardcfg.ShardConfig) {
	currentConfigStr, currentConfigVer, _ := sck.IKVClerk.Get("config")
	if currentConfigStr == "" {
		return
	}
	currentConfig := shardcfg.FromString(currentConfigStr)

	if newConfig.Num <= currentConfig.Num {
		return
	}

	_, nextConfigVer, _ := sck.IKVClerk.Get("next_config")
	sck.IKVClerk.Put("next_config", newConfig.String(), nextConfigVer)

	sck.moveShards(currentConfig, newConfig)
	sck.IKVClerk.Put("config", newConfig.String(), currentConfigVer)
}


// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	s, _, _ := sck.IKVClerk.Get("config")
	if s == "" {
		return nil
	}
	return shardcfg.FromString(s)
}

