package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"fmt"

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
}

type claimResult int

const (
	claimed    claimResult = iota
	notClaimed
)

type transferResult int

const (
	transferOK     transferResult = iota
	transferFailed
)

func nextConfigKey(num shardcfg.Tnum) string {
	return fmt.Sprintf("next_config_%d", num)
}

func (sck *ShardCtrler) tryClaimNextConfig(newConfig *shardcfg.ShardConfig) claimResult {
	pendingConfigKey := nextConfigKey(newConfig.Num)
	const createIfNotExists = rpc.Tversion(0)

	err := sck.IKVClerk.Put(pendingConfigKey, newConfig.String(), createIfNotExists)
	switch err {
	case rpc.OK:
		return claimed
	case rpc.ErrMaybe:
		return sck.confirmOwnWrite(pendingConfigKey, newConfig)
	default:
		return notClaimed
	}
}

func (sck *ShardCtrler) confirmOwnWrite(key string, expected *shardcfg.ShardConfig) claimResult {
	confirmedStr, _, _ := sck.IKVClerk.Get(key)
	if confirmedStr == expected.String() {
		return claimed
	}
	return notClaimed
}

func (sck *ShardCtrler) recoverPendingConfig(currentConfig *shardcfg.ShardConfig, currentConfigVer rpc.Tversion) {
	pendingConfigKey := nextConfigKey(currentConfig.Num + 1)
	pendingConfigStr, _, _ := sck.IKVClerk.Get(pendingConfigKey)
	if pendingConfigStr == "" {
		return
	}
	pendingConfig := shardcfg.FromString(pendingConfigStr)

	configAlreadyApplied := pendingConfig.Num <= currentConfig.Num
	if configAlreadyApplied {
		return
	}

	if sck.moveShards(currentConfig, pendingConfig) != transferOK {
		return
	}
	sck.IKVClerk.Put("config", pendingConfig.String(), currentConfigVer)
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	return sck
}

func needsMove(srcGroup, dstGroup tester.Tgid) bool {
	isUnassigned := srcGroup == 0 || dstGroup == 0
	return srcGroup != dstGroup && !isUnassigned
}

func (sck *ShardCtrler) moveShards(oldConfig, newConfig *shardcfg.ShardConfig) transferResult {
	for shardID := 0; shardID < shardcfg.NShards; shardID++ {
		srcGroup := oldConfig.Shards[shardID]
		dstGroup := newConfig.Shards[shardID]

		if !needsMove(srcGroup, dstGroup) {
			continue
		}

		if sck.transferShard(shardcfg.Tshid(shardID), newConfig.Num, oldConfig, srcGroup, newConfig, dstGroup) != transferOK {
			return transferFailed
		}
	}
	return transferOK
}

func (sck *ShardCtrler) transferShard(shardID shardcfg.Tshid, configNum shardcfg.Tnum, oldConfig *shardcfg.ShardConfig, srcGroup tester.Tgid, newConfig *shardcfg.ShardConfig, dstGroup tester.Tgid) transferResult {
	srcClerk := shardgrp.MakeClerk(sck.clnt, oldConfig.Groups[srcGroup])
	dstClerk := shardgrp.MakeClerk(sck.clnt, newConfig.Groups[dstGroup])

	state, err := srcClerk.FreezeShard(shardID, configNum)
	if err != rpc.OK {
		return transferFailed
	}

	err = dstClerk.InstallShard(shardID, state, configNum)
	if err != rpc.OK {
		return transferFailed
	}

	srcClerk.DeleteShard(shardID, configNum)
	return transferOK
}

func (sck *ShardCtrler) InitController() {
	currentConfigStr, currentConfigVer, _ := sck.IKVClerk.Get("config")
	if currentConfigStr == "" {
		return
	}
	currentConfig := shardcfg.FromString(currentConfigStr)
	sck.recoverPendingConfig(currentConfig, currentConfigVer)
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	sck.IKVClerk.Put("config", cfg.String(), 0)
}

func (sck *ShardCtrler) ChangeConfigTo(newConfig *shardcfg.ShardConfig) {
	currentConfigStr, currentConfigVer, _ := sck.IKVClerk.Get("config")
	if currentConfigStr == "" {
		return
	}
	currentConfig := shardcfg.FromString(currentConfigStr)

	configAlreadyApplied := newConfig.Num <= currentConfig.Num
	if configAlreadyApplied {
		return
	}

	if sck.tryClaimNextConfig(newConfig) != claimed {
		return
	}

	if sck.moveShards(currentConfig, newConfig) != transferOK {
		return
	}
	sck.IKVClerk.Put("config", newConfig.String(), currentConfigVer)
}

func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	configStr, _, _ := sck.IKVClerk.Get("config")
	if configStr == "" {
		return nil
	}
	return shardcfg.FromString(configStr)
}

