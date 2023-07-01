package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)


// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.

type Status int

type VoteState int 

type AppendEntriesState int

var HeartbeatInterval = 100 * time.Millisecond


type LogEntry struct {
	Term int
	Command interface{}
}

const (
	Follower Status = iota
	Candidate
	Leader
)

const (
	Normal VoteState = iota
	Killed
	Expired
	Voted
)

const (
	AppNormal AppendEntriesState = iota
	AppOutdated
	AppKilled
	AppExpired
	AppCommited
	Mismatch
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 2A
	//for all servers. each raft object represents a server (a node)
	currentTerm int
	votedFor int
	log []LogEntry

	committedIndex int
	lastApplied int

	//for leaders to modify followers
	nextIndex []int
	matchIndex []int

	//not included in the paper figure 2
	status Status
	overtime time.Duration
	timer *time.Ticker
	applyChan chan ApplyMsg

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.status == Leader

	return term, isleader
}


type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	AppState AppendEntriesState
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
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
	// Your code here (2C).
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


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
	VoteState VoteState
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed(){
		reply.VoteState = Killed
		reply.Term = -1
		reply.VoteGranted = false
		return
	}

	//if args outdated
	if args.Term < rf.currentTerm{
		reply.VoteState = Expired
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	//if args term is larger, the args node should be leader
	if args.Term > rf.currentTerm{
		//reset the current rf to be follower
		rf.status = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	//reset the vote after term is updated for this follower
	if rf.votedFor == -1 {
		currentLogIndex := len(rf.log) - 1
		currentLogTerm := 0
		if currentLogIndex >= 0 {
			//set log term if the log is not empty
			currentLogTerm = rf.log[currentLogIndex].Term
		}

		if args.LastLogIndex < currentLogIndex || args.LastLogTerm < currentLogTerm {
			reply.VoteState = Expired
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return
		}

		rf.votedFor = args.CandidateId
		reply.VoteState = Normal
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		
		rf.timer.Reset(rf.overtime)
	} else {
		reply.VoteState = Voted
		reply.VoteGranted = false
		
		if rf.votedFor != args.CandidateId {
			return //voted another candidate
		}else{
			//if voted, but the same candidate ask for vote again
			//reset the rf to followr
			rf.status = Follower //i think it doesnt matter if we have this line or not
		}
		rf.timer.Reset(rf.overtime) // vote send, reset the timer so it doesnt think no message received
	}
	
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, votedNums *int) bool {

	if rf.killed() {
		return false
	}

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	for !ok{
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}

	//print call success
	// fmt.Printf("server %d call server %d success\n", rf.me, server)
	
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//if the candidate args who request vote's term is lower, current raft server will not vote for it and return false
	if args.Term < rf.currentTerm {
		return false
	}


	switch reply.VoteState {
		//if the reply term is lower than the current term, the message is outdated
		case Expired:
			{
				rf.status = Follower
				rf.timer.Reset(rf.overtime)
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.votedFor = -1
				}
			}
		case Normal, Voted:
			if reply.VoteGranted && reply.Term == rf.currentTerm && *votedNums <= len(rf.peers) / 2 {
				*votedNums++
			}

			if *votedNums >= (len(rf.peers) / 2) + 1 {
				*votedNums = 0
				if rf.status == Leader{
					return ok
				}
				rf.status = Leader
				rf.nextIndex = make([]int, len(rf.peers))
				//if we set this to leader,  the later log index to be append is the last log index + 1
				for i, _ := range rf.nextIndex {
					rf.nextIndex[i] = len(rf.log) + 1 //cause log's first index is 1
				}
				rf.timer.Reset(HeartbeatInterval) //new leader, so heartbeat restart from now
			}
		case Killed:
			return false

	}
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

	// Your code here (2B).


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
	rf.mu.Lock()
	rf.timer.Stop()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		select{
			//if the timer is expired, start a new election
		case <-rf.timer.C:
			//if the server is killed, return and do nothing
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			switch rf.status {
				case Follower:
					rf.status = Candidate
					//fallthrough so it proceed to the candidate case
					fallthrough
				case Candidate:
					rf.currentTerm++
					rf.votedFor = rf.me
					votedNums := 1

					//reset the timer, but random the overtime
					rf.overtime = time.Duration(150 + rand.Intn(200)) * time.Millisecond
					rf.timer.Reset(rf.overtime)

					//send request vote to all other servers, ask for vote from them

					for i := 0; i < len(rf.peers); i++{
						//skip itself
						if i == rf.me {
							continue
						}

						voteArgs := RequestVoteArgs{
							Term: rf.currentTerm,
							CandidateId: rf.me,
							LastLogIndex: len(rf.log) - 1,
							//if the log is empty, the last log term is 0, else it is the last log entry's term
							LastLogTerm: 0, 
						}
						if len(rf.log) > 0 {
							voteArgs.LastLogTerm = rf.log[len(rf.log) - 1].Term
						}

						voteReply := RequestVoteReply{}
						
						go rf.sendRequestVote(i, &voteArgs, &voteReply, &votedNums)

					}
				case Leader:
					//send heartbeat to all other servers
					// appendNums := 1
					rf.timer.Reset(HeartbeatInterval)
					for i := 0; i < len(rf.peers); i++{
						if i == rf.me {
							continue
						}
						// appendEntriesArgs := AppendEntriesArgs{
						// 	Term: rf.currentTerm,
						// 	LeaderId: rf.me,
						// 	PrevLogIndex: 0,
						// 	PrevLogTerm: 0,
						// 	Entries: nil,
						// 	LeaderCommit: rf.committedIndex,
						// }
						// appendEntriesReply := AppendEntriesReply{}
						// go rf.sendAppendEntries(i, &appendEntriesArgs, &appendEntriesReply, &appendNums)
					}

			}
			rf.mu.Unlock()
		}
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyChan = applyCh

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)

	rf.committedIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.status = Follower
	rf.overtime = time.Duration(150 + rand.Intn(200)) * time.Millisecond
	rf.timer = time.NewTicker(rf.overtime)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
