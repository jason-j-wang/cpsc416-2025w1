Lab 3: Key/Value Server
==================

### Introduction

In this lab you will build a key/value server for a single machine that ensures that each Put operation is executed *at-most-once* despite network failures and that the operations are *linearizable*. You will use this KV server to implement a lock.

### KV Server

Each client interacts with the key/value server using a *Clerk*, which sends RPCs to the server. Clients can send two different RPCs to the server: `Put(key, value, version)` and `Get(key)`. The server maintains an in-memory map that records for each key a (value, version) tuple. Keys and values are strings. The version number records the number of times the key has been written. `Put(key, value, version)` installs or replaces the value for a particular key in the map *only if* the `Put`'s version number matches the server's version number for the key. If the version numbers match, the server also increments the version number of the key. If the version numbers don't match, the server should return `rpc.ErrVersion`. A client can create a new key by invoking `Put` with version number 0 (and the resulting version stored by the server will be 1). If the version number of the `Put` is larger than 0 and the key doesn't exist, the server should return `rpc.ErrNoKey`. 

`Get(key)` fetches the current value for the key and its associated version. If the key doesn't exist at the server, the server should return `rpc.ErrNoKey`.

Maintaining a version number for each key will be useful for implementing locks using `Put` and ensuring at-most-once semantics for `Put`'s when the network is unreliable and the client retransmits.

When you've finished this lab and passed all the tests, you'll have a *linearizable* key/value service from the point of view of clients calling `Clerk.Get` and `Clerk.Put`. That is, if client operations aren't concurrent, each client `Clerk.Get` and `Clerk.Put` will observe the modifications to the state implied by the preceding sequence of operations. For concurrent operations, the return values and final state will be the same as if the operations had executed one at a time in some order. Operations are concurrent if they overlap in time: for example, if client X calls `Clerk.Put()`, and client Y calls `Clerk.Put()`, and then client X's call returns. An operation must observe the effects of all operations that have completed before the operation starts. 

Linearizability is convenient for applications because it's the behavior you'd see from a single server that processes requests one at a time. For example, if one client gets a successful response from the server for an update request, subsequently launched reads from other clients are guaranteed to see the effects of that update. Providing linearizability is relatively easy for a single server.

### Getting Started

We supply you with skeleton code and tests in `src/kvsrv1`. `kvsrv1/client.go` implements a Clerk that clients use to manage RPC interactions with the server; the Clerk provides `Put` and `Get` methods. `kvsrv1/server.go` contains the server code, including the `Put` and `Get` handlers that implement the server side of RPC requests. You will need to modify `client.go` and `server.go`. The RPC requests, replies, and error values are defined in... the `kvsrv1/rpc` package in the file `kvsrv1/rpc/rpc.go`, which you should look at, though you don't have to modify `rpc.go`. 


    cd src/kvsrv1 
    go test -v 
    === RUN TestReliablePut 
    One client and reliable Put (reliable network)...       
    kvsrv_test.go:25: Put err ErrNoKey 
    ... 


### Key/Value Server with Reliable Network

Your first task is to implement a solution that works when there are no dropped messages. You'll need to add RPC-sending code to the Clerk Put/Get methods in `client.go`, and implement `Put` and `Get` RPC handlers in `server.go`.

You have completed this task when you pass the Reliable tests in the test suite:

    go test -v -run Reliable 
    === RUN TestReliablePut 
    One client and reliable Put (reliable network)... 
    ...Passed -- 0.0 1 5 0 
    --- PASS: TestReliablePut (0.00s) === RUN 
    TestPutConcurrentReliable Test: many clients racing to put values to the same key (reliable network)... 
    info: linearizability check timed out, assuming history is ok 
    ... Passed -- 3.1 1 90171 90171 
    --- PASS: TestPutConcurrentReliable (3.07s) 
    === RUN TestMemPutManyClientsReliable 
    Test: memory use many put clients (reliable network)... 
    ... Passed -- 9.2 1 100000 0 
    --- PASS: TestMemPutManyClientsReliable (16.59s) 
    PASS 
    ok cpsc416-2025w1/kvsrv1 19.681s...

The numbers after each `Passed` are real time in seconds, the constant 1, the number of RPCs sent (including client RPCs), and the number of key/value operations executed ( `Clerk` `Get` and `Put` calls).

### Implementing a Lock Using Key/Value Clerk

In many distributed applications, clients running on different machines use a key/value server to coordinate their activities. For example, ZooKeeper and Etcd allow clients to coordinate using a distributed lock, in analogy with how threads in a Go program can coordinate with locks (i.e., `sync.Mutex`). Zookeeper and Etcd implement such a lock with conditional put.

In this exercise your task is to implement a lock layered on client `Clerk.Put` and `Clerk.Get` calls. The lock supports two methods: `Acquire` and `Release`. The lock's specification is that only one client can successfully acquire the lock at a time; other clients must wait until the first client has released the lock using `Release`.

We supply you with skeleton code and tests in `src/kvsrv1/lock/`. You will need to modify `src/kvsrv1/lock/lock.go`. Your `Acquire` and `Release` code can talk... to your key/value server by calling `lk.ck.Put()` and `lk.ck.Get()`.

If a client crashes while holding a lock, the lock will never be released. In a design more sophisticated than this lab, the client would attach a [lease](https://en.wikipedia.org/wiki/Lease_(computer_science)#:~:text=Leases%20are%20commonly%20used%20in,to%20rely%20on%20the%20resource.) to a lock. When the lease expires, the lock server would release the lock on behalf of the client. In this lab clients don't crash and you can ignore this problem.

Implement `Acquire` and `Release`. You have completed this exercise when your code passes the Reliable tests in the test suite in the lock sub-directory:

    cd lock 
    go test -v -run Reliable 
    === RUN TestOneClientReliable 
    Test: 1 lock clients (reliable network)... 
    ... Passed -- 2.0 1 974 0 
    --- PASS: TestOneClientReliable (2.01s) 
    === RUN TestManyClientsReliable Test: 10 lock clients (reliable network)... 
    ... Passed -- 2.1 1 83194 0 
    --- PASS: TestManyClientsReliable (2.11s) 
    PASS 
    ok cpsc416-2025w1/kvsrv1/lock 4.120s

If you haven't implemented the lock yet, the first test will succeed.

This exercise requires little code but will require a bit more independent thought than the previous exercise.

- *Hint:* You will need a unique identifier for each lock client; call `kvtest.RandValue(8)` to generate a random string.
- *Hint:* The lock service should use a specific key to store the "lock state" (you would have to decide precisely what the lock state is). The key to be used is passed through the parameter `l` of MakeLock in `src/kvsrv1/lock/lock.go`.

### Key/Value Server with Dropped Messages

The main challenge in this exercise is that the network may re-order, delay, or discard RPC requests and/or replies. To recover from discarded requests/replies, the Clerk must keep re-trying each RPC until it receives a reply from the server.

If the network discards an RPC request message, then the client re-sending the request will solve the problem: the server will receive and execute just the re-sent request.

However, the network might instead discard an RPC reply message. The client does not know which message was discarded; the client only observes that it received no reply. If it was the reply that was discarded, and the client re-sends the RPC request, then the server will receive two copies of the request. That's OK for a `Get`, since `Get` doesn't modify the server state. It is safe to resend a `Put` RPC with the same version number, since the server executes `Put` conditionally on the version number; if the server received and executed a `Put` RPC, it will respond to a re-transmitted copy of that RPC with `rpc.ErrVersion` rather than executing the Put a second time.

A tricky case is if the server replies with an... `rpc.ErrVersion` in a response to an RPC that the Clerk retried. In this case, the Clerk cannot know if the Clerk's `Put` was executed by the server or not: the first RPC might have been executed by the server but the network may have discarded the successful response from the server, so that the server sent `rpc.ErrVersion` only for the retransmitted RPC. Or, it might be that another... Clerk updated the key before the Clerk's first RPC arrived at the server, so that the server executed neither of the Clerk's RPCs and replied `rpc.ErrVersion` to both. Therefore, if a Clerk receives `rpc.ErrVersion` for a retransmitted Put RPC, `Clerk.Put` must return `rpc.ErrMaybe` to the application instead of `rpc.ErrVersion` since the request may have been executed. It is then up to the application to handle this case. If the server responds to an initial (not retransmitted) Put RPC with `rpc.ErrVersion`, then the Clerk should return `rpc.ErrVersion` to the application, since the RPC was definitely not executed by the server.

It would be more convenient for application developers if `Put`'s were exactly-once (i.e., no `rpc.ErrMaybe` errors) but that is difficult to guarantee without maintaining state at the server for each Clerk. In the last exercise of this lab, you will implement a lock using your Clerk to explore how to program with at-most-once `Clerk.Put`.

Now you should modify your `kvsrv1/client.go` to continue in the face of dropped... RPC requests and replies. A return value of `true` from the client's `ck.clnt.Call()` indicates that the client received an RPC reply from the server; a return value of `false` indicates that it did not receive a reply (more precisely, `Call()` waits for a reply message for a timeout interval, and returns false if no reply arrives within that time). Your `Clerk` should keep re-sending an RPC until it receives a reply. Keep in mind the discussion of `rpc.ErrMaybe` above. Your solution shouldn't require any changes to the server.

Add code to `Clerk` to retry if doesn't receive a reply. Your have completed this task if your code passes all tests in `kvsrv1/`, like this:

    go test -v 
    === RUN TestReliablePut 
    One client and reliable Put (reliable network)... 
    ... Passed -- 0.0 1 5 0 
    --- PASS: TestReliablePut (0.00s) 
    === RUN TestPutConcurrentReliable 
    Test: many clients racing to put values to the same key (reliable network)... 
    info: linearizability check timed out, assuming history is ok 
    ... Passed -- 3.1 1 106647 106647 
    --- PASS: TestPutConcurrentReliable (3.09s) 
    === RUN TestMemPutManyClientsReliable 
    Test: memory use many put clients (reliable network)... 
    ... Passed -- 8.0 1 100000 0 
    --- PASS: TestMemPutManyClientsReliable (14.61s) 
    === RUN TestUnreliableNet 
    One client (unreliable network)... 
    ... Passed -- 7.6 1 251 208 
    --- PASS: TestUnreliableNet (7.60s) 
    PASS 
    ok cpsc416-2025w1/kvsrv1 25.319s.

- *Hint:* Before the client retries, it should wait a little bit; you can use go's `time` package and call `time.Sleep(100 * time.Millisecond)`.

### Implementing a Lock Using Key/Value Clerk and Unreliable Network

Modify your lock implementation to work correctly with your modified key/value client when the network is not reliable. You have completed this exercise when your code passes all the `kvsrv1/lock/` tests, including the unreliable ones:

    cd lock 
    go test -v 
    === RUN TestOneClientReliable 
    Test: 1 lock clients (reliable network)... 
    ... Passed -- 2.0 1 968 0 
    --- PASS: TestOneClientReliable (2.01s) 
    === RUN TestManyClientsReliable 
    Test: 10 lock clients (reliable network)... 
    ... Passed -- 2.1 1 10789 0 
    --- PASS: TestManyClientsReliable (2.12s) 
    === RUN TestOneClientUnreliable 
    Test: 1 lock clients (unreliable network)... 
    ... Passed -- 2.3 1 70 0 
    --- PASS: TestOneClientUnreliable (2.27s) 
    === RUN TestManyClientsUnreliable 
    Test: 10 lock clients (unreliable network)... 
    ... Passed -- 3.6 1 908 0 
    --- PASS: TestManyClientsUnreliable (3.62s) 
    PASS 
    ok cpsc416-2025w1/kvsrv1/lock 10.033s


## Submission Instructions

Add an INFO.md file inside the `kvsrv1` directory. If you used GenAI for this
lab, cite it (i.e., name the tool) and annotate it (i.e., briefly explain how
you used it) in this file. For example, "We asked ChatGPT for a simple
explanation of Go's RPC mechanism." or "We asked Cursor to generate (from
specifications in the prompt) all the arguments and reply structs in the
client.go file."

In the INFO.md file, also list all the team members and how you split up the
tasks. If you were pair (group) programming all the time, say so.

We expect you to only modify the `src/kvserv1/client.go`, `src/kvserv1/server.go`, and `src/kvsrv1/lock/lock.go` files.
You may add other files in the directory, such as `src/kvserv1/debug.go`,
but DO NOT modify any other existing file in a way that is critical for your
solution to work, as we will be replacing those files with their pristine
versions.

Since Canvas group submissions are not working as expected, this time I am
asking you to add student IDs in the name of the submission. Please strictly
adhere to the following format. To submit your solution, first compress the
`kvserv1` directory to create the `kvserv_studentid1_studentid2_studentid3.tar.gz`
file. For example, on Linux or macOS, if you are inside the `src/kvserv1/`
directory, you can run the following commands.
   ```bash
   cd ..
   tar -czvf kvserv_studentid1_studentid2_studentid3.tar.gz kvserv1/
   ```

If your team consists of two students or just one student, then rename the
file accoridngly, e.g., `kvserv_studentid1_studentid2.tar.gz` or
`kvserv_studentid1.tar.gz`.

Example submission names: `kvserv_11111111.tar.gz`, `kvserv_11111111_22222222.tar.gz`, `kvserv_11111111_22222222_33333333.tar.gz`.

This time, you may skip the group sign-up on Canvas, as it is not quite
working as intended. Simply have one of the team members submit the `*.tar.gz`
file when ready.
