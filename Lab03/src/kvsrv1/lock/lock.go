package lock

import (
	"cpsc416-2025w1/kvsrv1/rpc"
	kvtest "cpsc416-2025w1/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck       kvtest.IKVClerk
	key      string // key to identify the lock
	clientId string // unique identifier for this client
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:       ck,
		key:      l,
		clientId: kvtest.RandValue(8),
	}
	return lk
}

func (lk *Lock) Acquire() {
	for {
		value, version, err := lk.ck.Get(lk.key)

		if err == rpc.ErrNoKey {
			err = lk.ck.Put(lk.key, lk.clientId, 0)
			if err == rpc.OK {
				return
			}
			continue
		}

		if value == "" {
			err = lk.ck.Put(lk.key, lk.clientId, version)
			if err == rpc.OK {
				return
			}
		}
	}
}

func (lk *Lock) Release() {
	for {
		value, version, err := lk.ck.Get(lk.key)

		if err == rpc.ErrNoKey {
			return
		}

		if value == lk.clientId {
			err = lk.ck.Put(lk.key, "", version)
			if err == rpc.OK {
				return
			}
		} else {
			return
		}
	}
}
