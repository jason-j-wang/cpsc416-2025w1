package kvsrv

import (
	"log"
	"sync"

	"cpsc416-2025w1/kvsrv1/rpc"
	"cpsc416-2025w1/labrpc"
	tester "cpsc416-2025w1/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	mu sync.Mutex
	kvmap map[string]KVEntry
}

type KVEntry struct {
	value   string
	version rpc.Tversion
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	kv.kvmap = make(map[string]KVEntry)
	kv.mu = sync.Mutex{}
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value, ok := kv.kvmap[args.Key]

	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}

	reply.Value = value.value
	reply.Version = value.version
	reply.Err = rpc.OK
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	currentValue, ok := kv.kvmap[args.Key]
	
	if !ok {
		if args.Version != 0 {
			reply.Err = rpc.ErrNoKey
			return
		}
		// Key does not exist and version is 0, create new entry
		kv.kvmap[args.Key] = KVEntry{value: args.Value, version: 1}
		reply.Err = rpc.OK
		return
	}

	// Version mismatch
	if currentValue.version != args.Version {
		reply.Err = rpc.ErrVersion
		return
	}

	kv.kvmap[args.Key] = KVEntry{value: args.Value, version: currentValue.version + 1}
	reply.Err = rpc.OK
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}


// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
