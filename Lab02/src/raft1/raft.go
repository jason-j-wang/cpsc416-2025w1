package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"cpsc416-2025w1/labgob"
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


// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	numServers  int

	// 3A
	state	      string // "follower", "candidate", "leader"
	currentTerm   int
	votedFor      int   // -1 if no vote
	lastHeartbeat int64 // last heartbeat received from leader
	voteCount	  int	// number of votes received in current term

	log	      	  []LogEntry
	nextIndex	  []int // for each server, index of the next log entry to send to that server
	matchIndex	  []int // for each server, index of highest log entry known to be replicated on server

}

type LogEntry struct {
	Term int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).

	term = rf.currentTerm
	isleader = (rf.state == "leader")
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


// example RequestVote RPC arguments structure.
// field names must start with capital letters!

// Fields for each struct taken from the Raft paper
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term		 int
	CandidateId	 int
	LastLogIndex int
	LastLogTerm	 int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term 		int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term 		 int
	LeaderId 	 int
	PrevLogIndex int
	PrevLogTerm  int
	Entries 	 []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term 	int
	Success bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//debug(fmt.Sprintf("Sending AppendEntries to serverID %d", server), rf)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reject RPC if the current term is greater
	if args.Term < rf.currentTerm {
		//debug(fmt.Sprintf("Rejected AppendEntries from serverID %d for term %d", args.LeaderId, args.Term), rf)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	//debug(fmt.Sprintf("Accepted AppendEntries from serverID %d for term %d", args.LeaderId, args.Term), rf)
	rf.state = "follower"
	debug("no longer a leader (appendentries)", rf)
	rf.currentTerm = args.Term
	rf.votedFor = -1
	rf.lastHeartbeat = time.Now().UnixMilli()
	reply.Term = rf.currentTerm
	reply.Success = true
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 3A

	// Reject vote if term is less than current or we already voted this term
	if args.Term < rf.currentTerm  || (rf.currentTerm == args.Term && rf.votedFor != -1) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		//debug(fmt.Sprintf("Rejected vote request from serverID %d for term %d", args.CandidateId, args.Term), rf)
		return
	}
	//debug(fmt.Sprintf("Granted vote request from serverID %d for term %d", args.CandidateId, args.Term), rf)
	rf.state = "follower"
	debug("no longer a leader (requestvote)", rf)
	rf.votedFor = args.CandidateId
	rf.currentTerm = args.Term
	rf.lastHeartbeat = time.Now().UnixMilli()
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).


	return index, term, isLeader
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
	// Your code here, if desired.
	debug("Killed", rf)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) sendHeartbeatHelper(serverId int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.sendAppendEntries(serverId, args, reply)

	if ok {
		if reply.Success {
			//debug(fmt.Sprintf("Succesful AppendEntires to serverID %d", serverId), rf)

			// Only update the leader's own heartbeat when it receives a successful response
			// This way the leader knows it hasn't had a network failure
			rf.mu.Lock()
			rf.lastHeartbeat = time.Now().UnixMilli()
			rf.mu.Unlock()
		}

		// Turn leader to follower if the term from the response is higher
		if reply.Term > rf.currentTerm {
			debug(fmt.Sprintf("Discovered higher term %d from serverID %d during AppendEntries", reply.Term, serverId), rf)
			rf.mu.Lock()
			rf.state = "follower"
			debug("no longer a leader (sendheartbeat)", rf)
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.mu.Unlock()
		}
	} else {
		debug(fmt.Sprintf("AppendEntries RPC call to serverID %d failed", serverId), rf)
	}

}

func (rf *Raft) sendHeartbeat() {
	for rf.killed() == false {
		if rf.state == "leader" {
			debug(fmt.Sprintf("leader in term %d", rf.currentTerm), rf)
			for serverId := range rf.peers {
				// skip iteration if serverId is self
				if serverId == rf.me {
					continue
				}
				
				args := AppendEntriesArgs{}
				reply := AppendEntriesReply{}

				rf.mu.Lock()
			
				args.LeaderId = rf.me
				args.Term = rf.currentTerm
				// Heartbeat has no log entries
				args.Entries = []LogEntry{}
				prevLogIndex := rf.nextIndex[serverId] - 1

				prevLogTerm := 0
				if prevLogIndex >= 0 && prevLogIndex < len(rf.log) {
					prevLogTerm = rf.log[prevLogIndex].Term
				}

				leaderCommit := 0
				if len(rf.log) > 0 {
					leaderCommit = len(rf.log) - 1
				}

				args.PrevLogIndex = prevLogIndex
				args.PrevLogTerm = prevLogTerm
				args.LeaderCommit = leaderCommit

				rf.mu.Unlock()

				go rf.sendHeartbeatHelper(serverId, &args, &reply)
			}
		}

		// Case where leader has a network failure
		rf.mu.Lock()
		if rf.state == "leader" && time.Now().UnixMilli()-rf.lastHeartbeat > (ElectionTimeoutUpper + ElectionTimeoutLower)/2 {
			debug("Leader timed out (lost majority), stepping down", rf)
			rf.state = "follower"
			rf.votedFor = -1
		}
		rf.mu.Unlock()

		time.Sleep(time.Duration(200) * time.Millisecond)
	}
}

func (rf *Raft) voteRequestHelper(serverId int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.sendRequestVote(serverId, args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		if reply.VoteGranted {
			
			rf.voteCount += 1
			
			//debug(fmt.Sprintf("Received vote from serverID %d", serverId), rf)

			// it is possible that while waiting for votes, another server becomes the leader and 
			// rf.state will change to follower via AppendEntries RPC
			if rf.state == "candidate" && rf.voteCount > rf.numServers/2 {
				debug(fmt.Sprintf("Becoming leader for term %d", rf.currentTerm), rf)
				rf.state = "leader"
				rf.votedFor = -1
			}
		}

		// End the election if the term from the response is higher
		if reply.Term > rf.currentTerm {
			debug(fmt.Sprintf("Discovered higher term %d from serverID %d during RequestVote", reply.Term, serverId), rf)
			rf.state = "follower"
			debug("no longer a leader (vote request hleper)", rf)
			rf.currentTerm = reply.Term
			rf.voteCount = 0
			rf.votedFor = -1
		}
	} else {
		debug(fmt.Sprintf("RequestVote RPC call to serverID %d failed", serverId), rf)
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		// random timeout value
		leaderTimeout := ElectionTimeoutLower + (rand.Int63() % (ElectionTimeoutUpper - ElectionTimeoutLower))

		// Start election
		if (rf.state != "leader" && time.Now().UnixMilli() - rf.lastHeartbeat > leaderTimeout) {

			rf.mu.Lock()
			rf.state = "candidate"
			debug("no longer a leader (becomes a candidate)", rf)
			rf.currentTerm += 1
			rf.votedFor = rf.me
			rf.voteCount = 1
			rf.mu.Unlock()

			// For optimization: update lastHeartbeat so server won't instantly start another election
			rf.lastHeartbeat = time.Now().UnixMilli()
			debug(fmt.Sprintf("Starting election for term %d", rf.currentTerm), rf)

			for serverId := range rf.peers {
				// skip iteration if serverId is self
				if serverId == rf.me {
					continue
				}

				args := RequestVoteArgs{}
				reply := RequestVoteReply{}

				args.CandidateId = rf.me
				args.Term = rf.currentTerm
				lastLogIndex := len(rf.log)

				lastLogTerm := 0
				if lastLogIndex > 0 {
					lastLogTerm = rf.log[lastLogIndex-1].Term
				}
				args.LastLogIndex = lastLogIndex
				args.LastLogTerm = lastLogTerm

				go rf.voteRequestHelper(serverId, &args, &reply)
			}
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.numServers = len(peers)

	// 3A
	rf.state = "follower"
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.log = make([]LogEntry, 0)
	rf.nextIndex = make([]int, rf.numServers)
	rf.matchIndex = make([]int, rf.numServers)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.sendHeartbeat()

	return rf
}
