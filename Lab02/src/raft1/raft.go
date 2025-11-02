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
	log         []LogEntry

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
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var votedFor int
	var currentTerm int
	var persistLog []LogEntry

	if d.Decode(&votedFor) != nil || 
		d.Decode(&currentTerm) != nil ||
		d.Decode(&persistLog) != nil {
			log.Fatal("Failed to decode persisted state")
	} else {
	  rf.votedFor = votedFor
	  rf.currentTerm = currentTerm
	  rf.log = persistLog
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
	// Your code here (3D).

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

	// Reply false if log doesn't contain an entry at prevLogIndex
	if args.PrevLogIndex >= len(rf.log) {
		reply.XLen = len(rf.log)
		reply.XTerm = -1
		reply.XIndex = -1
		return
	}

	// Reply false if log entry at prevLogIndex has wrong term
	if args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.XTerm = rf.log[args.PrevLogIndex].Term
		reply.XIndex = args.PrevLogIndex
		// Find first index with XTerm
		for i := args.PrevLogIndex; i >= 1; i-- {
			if rf.log[i].Term != reply.XTerm {
				reply.XIndex = i + 1
				break
			}
			if i == 1 {
				reply.XIndex = 1
			}
		}
		reply.XLen = len(rf.log)
		return
	}

	// If an existing entry conflicts with a new one, delete it and all following
	insertIndex := args.PrevLogIndex + 1
	for i, entry := range args.Entries {
		logIndex := insertIndex + i
		if logIndex < len(rf.log) {
			if rf.log[logIndex].Term != entry.Term {
				// Delete conflicting entry and all that follow
				rf.log = rf.log[:logIndex]
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
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log)-1)))
		rf.applyCond.Signal()
	}

	reply.Success = true
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

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 1 {
		lastLogTerm = rf.log[lastLogIndex].Term
	}

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

	index := len(rf.log)
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
		if reply.XTerm == -1 {
			rf.nextIndex[serverId] = reply.XLen
		} else {
			foundTerm := false
			for i := len(rf.log) - 1; i >= 1; i-- {
				if rf.log[i].Term == reply.XTerm {
					rf.nextIndex[serverId] = i + 1
					foundTerm = true
					break
				}
			}
			if !foundTerm {
				rf.nextIndex[serverId] = reply.XIndex
			}
		}

		// Ensure nextIndex doesn't go below 1 (since index 0 is dummy)
		if rf.nextIndex[serverId] < 1 {
			rf.nextIndex[serverId] = 1
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

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
		}

		nextIdx := rf.nextIndex[serverId]
		args.PrevLogIndex = nextIdx - 1

		if args.PrevLogIndex >= 0 && args.PrevLogIndex < len(rf.log) {
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
		}

		if nextIdx < len(rf.log) {
			args.Entries = make([]LogEntry, len(rf.log)-nextIdx)
			copy(args.Entries, rf.log[nextIdx:])
		}

		reply := AppendEntriesReply{}
		go rf.sendAppendEntriesHelper(serverId, &args, &reply)
	}
	rf.mu.Unlock()
}

func (rf *Raft) tryAdvanceCommitIndex() {
	for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
		if n < 1 || rf.log[n].Term != rf.currentTerm {
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
				rf.nextIndex[i] = len(rf.log)
				rf.matchIndex[i] = 0
			}
			rf.matchIndex[rf.me] = len(rf.log) - 1

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

			lastLogIndex := len(rf.log) - 1
			if lastLogIndex >= 1 {
				args.LastLogIndex = lastLogIndex
				args.LastLogTerm = rf.log[lastLogIndex].Term
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
		for i := startIndex; i <= rf.commitIndex; i++ {
			entries = append(entries, rf.log[i])
		}
		rf.lastApplied = rf.commitIndex
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

	rf.nextIndex = make([]int, rf.numServers)
	rf.matchIndex = make([]int, rf.numServers)
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}

	rf.lastHeartbeat = time.Now().UnixMilli()
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()
	go rf.sendHeartbeat()
	go rf.applier()

	return rf
}
