package raft

import (
	"bytes"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"cpsc416-2025w1/labgob"
	"cpsc416-2025w1/labrpc"
	"cpsc416-2025w1/raftapi"
	tester "cpsc416-2025w1/tester1"
)

// Constants
var EnableDebug bool = false
var ElectionTimeoutLower int64 = 1000
var ElectionTimeoutUpper int64 = 3000

func debug(text string, rf *Raft) {
	if EnableDebug && !rf.killed() {
		log.Printf("Server %d: %s", rf.me, text)
	}
}

type RaftState string

const (
	RaftStateFollower  RaftState = "FOLLOWER"
	RaftStateCandidate RaftState = "CANDIDATE"
	RaftStateLeader    RaftState = "LEADER"
)

type Raft struct {
	// Needed state
	mu         sync.Mutex
	peers      []*labrpc.ClientEnd
	persister  *tester.Persister
	me         int
	dead       int32
	numServers int

	// Persistent state on all servers
	state       RaftState
	currentTerm int
	votedFor    int
	// Now stores the log entries starting at lastIncludedIndex + 1, older logs we do not care as they should be stored in the snapshot data
	log         []LogEntry


	lastIncludedIndex  int // highest log index included in the snapshot.
	lastIncludedTerm   int // term of that log entry.
	snapshot           []byte // current snapshot


	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// Other state
	lastHeartbeat int64
	voteCount     int
	applyCh       chan raftapi.ApplyMsg
	applyCond     *sync.Cond
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// helper to convert between global log index and rf.log slice indices
func (rf *Raft) sliceIndex(globalIdx int) int {
	return globalIdx - rf.lastIncludedIndex
}

// last global log index
func (rf *Raft) lastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}

// last log term (global)
func (rf *Raft) lastLogTerm() int {
	if len(rf.log) > 1 {
		return rf.log[len(rf.log)-1].Term
	}
	return rf.lastIncludedTerm
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := (rf.state == RaftStateLeader)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	if rf.snapshot != nil {
		rf.persister.Save(raftstate, rf.snapshot)
	} else {
		rf.persister.Save(raftstate, nil)
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var votedFor int
	var currentTerm int
	var persistLog []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int


	if d.Decode(&votedFor) != nil ||
		d.Decode(&currentTerm) != nil ||
		d.Decode(&persistLog) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		log.Fatal("Failed to decode persisted state")
	}

	// restore decoded raft state
	rf.votedFor = votedFor
	rf.currentTerm = currentTerm
	rf.log = persistLog
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm

	snap := rf.persister.ReadSnapshot()
	if snap != nil {
		rf.snapshot = snap
	} else {
		rf.snapshot = nil
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Ignore old snapshots
	if index <= rf.lastIncludedIndex {
		return
	}

	// pos refers to the index in rf.log corresponding to the global index (ie it translates from global index to local)
	pos := rf.sliceIndex(index)
	

	// Gets term of the entry being included in the snapshot (do this before trimming)
	var cutTerm int
	if pos < len(rf.log) {
		cutTerm = rf.log[pos].Term
	} else {
		// This shouldnt really happen i think, cause that means we are snapshotting beyond our log, which shouldnt be possible
		// I believe this is called from the service layer which would have to know the log entry so i dont think this should be possible
		// but just in case we handle it
		cutTerm = rf.lastIncludedTerm
	}


	newLog := make([]LogEntry, 1)
	newLog[0] = LogEntry{Term: 0, Command: nil}
	if pos+1 < len(rf.log) {
		newLog = append(newLog, rf.log[pos+1:]...)
	}
	rf.log = newLog

	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = cutTerm
	rf.snapshot = snapshot

	// Ensure commitIndex/lastApplied don't lag behind snapshot
	if rf.commitIndex < index {
		rf.commitIndex = index
	}
	if rf.lastApplied < index {
		rf.lastApplied = index
	}

	rf.persist()
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int // Term of conflicting entry
	XIndex  int // Index of first entry with XTerm
	XLen    int // Log length
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	rf.lastHeartbeat = time.Now().UnixMilli()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = RaftStateFollower
		rf.persist()
		debug("converted to follower (appendentries)", rf)
	}

	if rf.state != RaftStateFollower {
		rf.state = RaftStateFollower
		debug("no longer a leader (appendentries)", rf)
	}

	// If PrevLogIndex is before our snapshot, leader should send snapshot instead
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.XLen = rf.lastIncludedIndex
		reply.XTerm = -1
		reply.XIndex = -1
		return
	}

	globalLast := rf.lastLogIndex()
	// if PrevLogIndex is beyond our last index that means we are missing entries so we reply starting at the global last +1
	if args.PrevLogIndex > globalLast {
		reply.XLen = globalLast + 1
		return
	}

	// check PrevLogTerm (handle PrevLogIndex == lastIncludedIndex)
	if args.PrevLogIndex == rf.lastIncludedIndex {
		// Reply false if the prevLogTerm and our recorded last term are not matching
		if args.PrevLogTerm != rf.lastIncludedTerm {
			reply.XTerm = rf.lastIncludedTerm
			reply.XIndex = rf.lastIncludedIndex
			reply.XLen = globalLast + 1
			return
		}
	} else {
		pos := rf.sliceIndex(args.PrevLogIndex)
		// That means Pos is out of bounds of our local log so we reject as we need the data starting from Globallast +1
		if pos < 0 || pos >= len(rf.log) {
			reply.XLen = globalLast + 1
			return
		}
		if rf.log[pos].Term != args.PrevLogTerm {
			reply.XTerm = rf.log[pos].Term
			// find first index with XTerm
			i := args.PrevLogIndex
			for i >= rf.lastIncludedIndex+1 {
				if rf.log[rf.sliceIndex(i)].Term != reply.XTerm {
					reply.XIndex = i + 1
					break
				}
				if i == rf.lastIncludedIndex+1 {
					reply.XIndex = rf.lastIncludedIndex + 1
					break
				}
				i--
			}
			reply.XLen = globalLast + 1
			return
		}
	}

	// insert entries starting at PrevLogIndex + 1
	insertIndex := args.PrevLogIndex + 1
	for i, entry := range args.Entries {
		logIndex := insertIndex + i
		pos := rf.sliceIndex(logIndex)
		if pos < len(rf.log) {
			if rf.log[pos].Term != entry.Term {
				// Delete conflicting entry and all that follow
				rf.log = rf.log[:pos]
				rf.log = append(rf.log, args.Entries[i:]...)
				rf.persist()
				break
			}
		} else {
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(rf.lastLogIndex())))
		rf.applyCond.Signal()
	}

	reply.Success = true
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
    ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
    return ok
}

func (rf *Raft) sendInstallSnapshotHelper(serverId int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
    ok := rf.sendInstallSnapshot(serverId, args, reply)
    if !ok {
        return
    }

    rf.mu.Lock()
    defer rf.mu.Unlock()

    if rf.state != RaftStateLeader || args.Term != rf.currentTerm {
        return
    }

    if reply.Term > rf.currentTerm {
        rf.currentTerm = reply.Term
        rf.votedFor = -1
        rf.state = RaftStateFollower
        rf.persist()
        return
    }

    // Update nextIndex and matchIndex after successful snapshot installation
    rf.nextIndex[serverId] = args.LastIncludedIndex + 1
    rf.matchIndex[serverId] = args.LastIncludedIndex
}

// InstallSnapshot RPC handler. Leader sends this when a follower is too far
// behind and the leader has a snapshot covering the follower's missing entries.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	reply.Term = rf.currentTerm

	// Update last heartbeat since we got a message from current leader
	rf.lastHeartbeat = time.Now().UnixMilli()

	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	// If leader's term is newer, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = RaftStateFollower
		rf.persist()
	}

	if rf.state != RaftStateFollower {
		rf.state = RaftStateFollower
	}

	// If the snapshot is older than or equal to what we have, ignore it
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		return
	}

	// Save current state before making changes
	oldCommitIndex := rf.commitIndex
	oldLastApplied := rf.lastApplied
	oldSnapshot := rf.snapshot
	oldLastIncludedIndex := rf.lastIncludedIndex
	oldLastIncludedTerm := rf.lastIncludedTerm

	// Create new log starting with dummy entry
	newLog := make([]LogEntry, 1)
	newLog[0] = LogEntry{Term: 0, Command: nil}

	// Try to retain log entries after snapshot point
	if args.LastIncludedIndex <= rf.lastLogIndex() {
		pos := rf.sliceIndex(args.LastIncludedIndex)
		if pos >= 0 && pos < len(rf.log) {
			if rf.log[pos].Term == args.LastIncludedTerm {
				newLog = append(newLog, rf.log[pos+1:]...)
			}
		}
	}

	// Update state
	rf.log = newLog
	rf.snapshot = make([]byte, len(args.Data))
	copy(rf.snapshot, args.Data)
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	// Update commit and apply indices
	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}

	rf.persist()

	// Prepare snapshot message
	msg := raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      make([]byte, len(args.Data)),
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	copy(msg.Snapshot, args.Data)

	// If something goes wrong while sending the snapshot,
	// we need to roll back our state
	select {
	case rf.applyCh <- msg:
		// Successfully sent the snapshot, we can release the lock
		rf.mu.Unlock()
	default:
		// Channel blocked, roll back changes
		rf.log = make([]LogEntry, 1)
		rf.log[0] = LogEntry{Term: 0, Command: nil}
		rf.snapshot = oldSnapshot
		rf.lastIncludedIndex = oldLastIncludedIndex
		rf.lastIncludedTerm = oldLastIncludedTerm
		rf.commitIndex = oldCommitIndex
		rf.lastApplied = oldLastApplied
		rf.persist()
		rf.mu.Unlock()
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = RaftStateFollower
		rf.persist()
		debug("converted to follower (requestvote)", rf)
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		return
	}

	lastLogIndex := rf.lastLogIndex()
	lastLogTerm := rf.lastLogTerm()

	isUpToDate := args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if !isUpToDate {
		return
	}

	rf.votedFor = args.CandidateId
	rf.lastHeartbeat = time.Now().UnixMilli()
	reply.VoteGranted = true
	rf.persist()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != RaftStateLeader || rf.killed() {
		return -1, rf.currentTerm, false
	}

	index := rf.lastLogIndex() + 1
	term := rf.currentTerm
	rf.log = append(rf.log, LogEntry{Term: term, Command: command})
	rf.persist()

	go rf.sendAppendEntriesToAll()

	return index, term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	debug("Killed", rf)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) sendAppendEntriesHelper(serverId int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.sendAppendEntries(serverId, args, reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != RaftStateLeader || args.Term != rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = RaftStateFollower
		rf.persist()
		debug("converted to follower (heartbeat reply)", rf)
		return
	}

	if reply.Success {
		newMatchIndex := args.PrevLogIndex + len(args.Entries)
		if newMatchIndex > rf.matchIndex[serverId] {
			rf.matchIndex[serverId] = newMatchIndex
			rf.nextIndex[serverId] = newMatchIndex + 1
		}

		rf.tryAdvanceCommitIndex()
	} else {
		// Check if the follower needs a snapshot
		if reply.XLen <= rf.lastIncludedIndex {
			args := InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm:  rf.lastIncludedTerm,
				Data:              make([]byte, len(rf.snapshot)),
			}
			copy(args.Data, rf.snapshot)
			snapshotReply := InstallSnapshotReply{}
			go rf.sendInstallSnapshotHelper(serverId, &args, &snapshotReply)
			return
		}

		if reply.XTerm == -1 {
			rf.nextIndex[serverId] = reply.XLen
		} else {
			foundTerm := false
			// search global indices backwards
			for gi := rf.lastLogIndex(); gi >= rf.lastIncludedIndex+1; gi-- {
				if rf.log[rf.sliceIndex(gi)].Term == reply.XTerm {
					rf.nextIndex[serverId] = gi + 1
					foundTerm = true
					break
				}
			}
			if !foundTerm {
				rf.nextIndex[serverId] = reply.XIndex
			}
		}

		if rf.nextIndex[serverId] < rf.lastIncludedIndex+1 {
			rf.nextIndex[serverId] = rf.lastIncludedIndex + 1
		}
	}
}

func (rf *Raft) sendAppendEntriesToAll() {
	rf.mu.Lock()
	if rf.state != RaftStateLeader {
		rf.mu.Unlock()
		return
	}

	for serverId := range rf.peers {
		if serverId == rf.me {
			continue
		}

		nextIdx := rf.nextIndex[serverId]
		
		// If the next index we need to send is less than or equal to our snapshot's last index
		// we need to send the snapshot instead of log entries
		if nextIdx <= rf.lastIncludedIndex {
			args := InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm:  rf.lastIncludedTerm,
				Data:              make([]byte, len(rf.snapshot)),
			}
			copy(args.Data, rf.snapshot)
			reply := InstallSnapshotReply{}
			go rf.sendInstallSnapshotHelper(serverId, &args, &reply)
			continue
		}

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
		}

		args.PrevLogIndex = nextIdx - 1

		if args.PrevLogIndex == rf.lastIncludedIndex {
			args.PrevLogTerm = rf.lastIncludedTerm
		} else if args.PrevLogIndex > rf.lastIncludedIndex {
			pos := rf.sliceIndex(args.PrevLogIndex)
			if pos >= 0 && pos < len(rf.log) {
				args.PrevLogTerm = rf.log[pos].Term
			}
		}

		if nextIdx <= rf.lastLogIndex() {
			start := rf.sliceIndex(nextIdx)
			if start >= 0 && start < len(rf.log) {
				args.Entries = make([]LogEntry, len(rf.log)-start)
				copy(args.Entries, rf.log[start:])
			}
		}

		reply := AppendEntriesReply{}
		go rf.sendAppendEntriesHelper(serverId, &args, &reply)
	}
	rf.mu.Unlock()
}

func (rf *Raft) tryAdvanceCommitIndex() {
	for n := rf.lastLogIndex(); n > rf.commitIndex; n-- {
		if n < rf.lastIncludedIndex+1 {
			continue
		}
		if rf.log[rf.sliceIndex(n)].Term != rf.currentTerm {
			continue
		}

		count := 1 // Count self
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= n {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			rf.applyCond.Signal()
			break
		}
	}
}

func (rf *Raft) sendHeartbeat() {
	for rf.killed() == false {
		rf.mu.Lock()
		isLeader := rf.state == RaftStateLeader
		rf.mu.Unlock()

		if isLeader {
			rf.sendAppendEntriesToAll()
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) voteRequestHelper(serverId int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.sendRequestVote(serverId, args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok {
		return
	}

	if rf.state != RaftStateCandidate || args.Term != rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = RaftStateFollower
		rf.persist()
		debug("converted to follower (vote reply)", rf)
		return
	}

	if reply.VoteGranted {
		rf.voteCount++

		if rf.voteCount > rf.numServers/2 {
			debug(fmt.Sprintf("Becoming leader for term %d", rf.currentTerm), rf)
			rf.state = RaftStateLeader

			for i := range rf.peers {
				rf.nextIndex[i] = rf.lastLogIndex() + 1
				rf.matchIndex[i] = rf.lastIncludedIndex
			}
			rf.matchIndex[rf.me] = rf.lastLogIndex()

			go rf.sendAppendEntriesToAll()
		}
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		leaderTimeout := ElectionTimeoutLower + (rand.Int63() % (ElectionTimeoutUpper - ElectionTimeoutLower))

		rf.mu.Lock()
		isLeader := rf.state == RaftStateLeader
		timeSinceHeartbeat := time.Now().UnixMilli() - rf.lastHeartbeat
		rf.mu.Unlock()

		if !isLeader && timeSinceHeartbeat > leaderTimeout {
			rf.mu.Lock()
			rf.state = RaftStateCandidate
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.voteCount = 1
			rf.lastHeartbeat = time.Now().UnixMilli()
			rf.persist()

			args := RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateId: rf.me,
			}

			lastLogIndex := rf.lastLogIndex()
			if lastLogIndex >= rf.lastIncludedIndex+1 {
				args.LastLogIndex = lastLogIndex
				args.LastLogTerm = rf.lastLogTerm()
			}

			debug(fmt.Sprintf("Starting election for term %d", rf.currentTerm), rf)
			rf.mu.Unlock()

			for serverId := range rf.peers {
				if serverId == rf.me {
					continue
				}
				reply := RequestVoteReply{}
				go rf.voteRequestHelper(serverId, &args, &reply)
			}
		}

		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()

		// Wait for commitIndex > lastApplied
		for rf.commitIndex <= rf.lastApplied && !rf.killed() {
			rf.applyCond.Wait()
		}

		if rf.killed() {
			rf.mu.Unlock()
			return
		}

		entries := make([]LogEntry, 0)
		startIndex := rf.lastApplied + 1
		
		// Safety check: don't try to apply entries before snapshot
		if startIndex <= rf.lastIncludedIndex {
			startIndex = rf.lastIncludedIndex + 1
		}

		// Only proceed if there are entries to apply
		if startIndex <= rf.commitIndex {
			for i := startIndex; i <= rf.commitIndex; i++ {
				// Only append entries that are after our snapshot
				if i > rf.lastIncludedIndex {
					sliceIdx := rf.sliceIndex(i)
					if sliceIdx >= 0 && sliceIdx < len(rf.log) {
						entries = append(entries, rf.log[sliceIdx])
					}
				}
			}
			rf.lastApplied = rf.commitIndex
		}
		rf.mu.Unlock()

		// Send to applyCh outside lock
		for i, entry := range entries {
			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: startIndex + i,
			}
			rf.applyCh <- msg
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.numServers = len(peers)
	rf.applyCh = applyCh

	rf.state = RaftStateFollower
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{Term: 0, Command: nil}
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.readPersist(persister.ReadRaftState())

	rf.nextIndex = make([]int, rf.numServers)
	rf.matchIndex = make([]int, rf.numServers)
	for i := range rf.matchIndex {
		rf.matchIndex[i] = rf.lastIncludedIndex
	}

	rf.lastHeartbeat = time.Now().UnixMilli()
	rf.applyCond = sync.NewCond(&rf.mu)

	go rf.ticker()
	go rf.sendHeartbeat()
	go rf.applier()

	return rf
}
