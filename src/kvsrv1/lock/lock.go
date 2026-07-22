package lock

import (
	"crypto/rand"
	"fmt"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

const (
	StateLocked   = "locked"
	StateUnlocked = "unlocked"

	retryInterval = 100 * time.Millisecond
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck   kvtest.IKVClerk
	name string
	id   string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("crypto/rand failed: %v", err))
	}
	return &Lock{
		ck:   ck,
		name: lockname,
		id:   fmt.Sprintf("%x", b),
	}
}

func (lk *Lock) Acquire() {
	for {
		if lk.tryAcquire() {
			return
		}

		time.Sleep(retryInterval)
	}
}

func (lk *Lock) tryAcquire() bool {
	val, ver, err := lk.ck.Get(lk.name)

	isAvailable := (err == rpc.ErrNoKey) || (err == rpc.OK && val == StateUnlocked)
	if !isAvailable {
		return false
	}

	var putVer rpc.Tversion
	if err == rpc.OK {
		putVer = ver
	}

	putErr := lk.ck.Put(lk.name, lk.id, putVer)

	if putErr == rpc.OK {
		return true
	}

	isAmbiguousSuccess := putErr == rpc.ErrMaybe
	if isAmbiguousSuccess {
		newVal, _, newErr := lk.ck.Get(lk.name)
		isNowLockedByMe := newErr == rpc.OK && newVal == lk.id
		return isNowLockedByMe
	}

	return false
}

func (lk *Lock) Release() {
	for {
		val, ver, err := lk.ck.Get(lk.name)

		if err == rpc.OK {
			unlockAlreadySucceeded := val != lk.id
			if unlockAlreadySucceeded {
				return
			}

			if lk.ck.Put(lk.name, StateUnlocked, ver) == rpc.OK {
				return
			}
		}

		time.Sleep(retryInterval)
	}
}
