package kvsrv

import (
	"time"

	"cpsc416-2025w1/kvsrv1/rpc"
	kvtest "cpsc416-2025w1/kvtest1"
	tester "cpsc416-2025w1/tester1"
)

type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	// You may add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}

	for {
		reply := rpc.GetReply{}
		ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)

		if ok && reply.Err == rpc.OK {
			return reply.Value, reply.Version, rpc.OK
		}

		if ok && reply.Err == rpc.ErrNoKey {
			return "", 0, rpc.ErrNoKey
		}

		// Network failure, retry after a delay
		time.Sleep(100 * time.Millisecond)
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	firstAttempt := true

	for {
		reply := rpc.PutReply{}
		ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)

		if !ok {
			// Network failure - retry after delay
			firstAttempt = false
			time.Sleep(100 * time.Millisecond)
			continue
		}
		
		// Successful 
		if reply.Err == rpc.OK {
			return rpc.OK
		}

		// Incorrect version
		if reply.Err == rpc.ErrVersion {
			if firstAttempt {
				return rpc.ErrVersion
			} else {
				return rpc.ErrMaybe
			}
		}

		// Key does not exist
		if reply.Err == rpc.ErrNoKey {
			return rpc.ErrNoKey
		}
		
		firstAttempt = false
	}
}
