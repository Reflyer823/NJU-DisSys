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
	// "fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

const (
	STATE_FOLLOWER  = 0
	STATE_CANDIDATE = 1
	STATE_LEADER    = 2
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm  int
	votedFor     int
	log          []LogEntry
	commitIndex  int
	lastApplied  int
	state        int
	nextIndex    []int
	matchIndex   []int
	heartBeat    chan bool
	elecTimeOut  time.Duration
	sumVotes     int
	stateChanged chan bool
}

type LogEntry struct {
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, (rf.state == STATE_LEADER)
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// check the term received
//
func (rf *Raft) checkTerm(newTerm int) {
	isStateChanged := false
	if newTerm > rf.currentTerm {
		rf.mu.Lock()
		rf.currentTerm = newTerm	// update term
		if rf.state != STATE_FOLLOWER {
			isStateChanged = true
		}
		rf.state = STATE_FOLLOWER	// convert to follower
		rf.votedFor = -1			// clear voted candidate
		rf.mu.Unlock()
		if isStateChanged {
			rf.stateChanged <- true
		}
	}
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.checkTerm(args.Term)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogIndex >= rf.commitIndex && args.LastLogTerm >= rf.currentTerm {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			// fmt.Printf("Raft %v votes for Raft %v\n", rf.me, args.CandidateId)
		}
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := rf.state == STATE_LEADER

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// AppendEntries RPC arguments structure.
//
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

//
// AppendEntries RPC reply structure.
//
type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// AppendEntries RPC Handler
//
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// fmt.Printf("Raft %v received heartbeat from Raft %v.\n", rf.me, args.LeaderId)
	rf.checkTerm(args.Term)
	if rf.state == STATE_FOLLOWER {
		rf.heartBeat <- true
	}
	if rf.state == STATE_CANDIDATE && args.Term == rf.currentTerm {
		rf.mu.Lock()
		rf.state = STATE_FOLLOWER	// convert to follower
		rf.mu.Unlock()
		rf.stateChanged <- true
	}
	reply.Term = rf.currentTerm
	reply.Success = true
}

// 
// Raft server main thread
// 
func (rf *Raft) run() {
	for {
		switch rf.state {
		case STATE_FOLLOWER:  // follower
			select {
			case <-rf.heartBeat:
			case <-time.After(rf.elecTimeOut):
				// fmt.Printf("Raft %v election time out\n", rf.me)
				// election time out, convert to candidate
				rf.mu.Lock()
				rf.state = STATE_CANDIDATE
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.sumVotes = 1
				rf.mu.Unlock()
			}
		case STATE_CANDIDATE:  // candidate
			// start election
			electionTimer := time.After(rf.elecTimeOut)
			args := RequestVoteArgs{
				Term: rf.currentTerm,
				CandidateId: rf.me,
				LastLogIndex: rf.commitIndex,
				LastLogTerm: rf.currentTerm}
			// send RequestVote RPC to every node
			for i := range rf.peers {
				// skip candidate itself
				if i == rf.me {
					continue
				}
				// init a new goroutine to send RequestVote
				go func(i int) {
					reply := RequestVoteReply{}
					if ok := rf.sendRequestVote(i, args, &reply); ok {
						rf.checkTerm(reply.Term)
						if reply.VoteGranted && rf.state == STATE_CANDIDATE &&
						rf.currentTerm == args.Term {
							rf.mu.Lock()
							rf.sumVotes++
							if rf.sumVotes > len(rf.peers) / 2 {
								rf.state = STATE_LEADER
								rf.stateChanged <- true
								// fmt.Printf("Raft %v becomes leader\n", rf.me)
							}
							rf.mu.Unlock()
						}
					}
				}(i)
			}
			// wait until state changed or election timeout
			select {
			case <-rf.stateChanged:
			case <-electionTimer:
				rf.mu.Lock()
				rf.currentTerm++
				rf.mu.Unlock()
			}
			// fmt.Printf("Raft %v election ends with %v support\n", rf.me, rf.sumVotes)
		case STATE_LEADER:  // leader
			args := AppendEntriesArgs{
				Term: rf.currentTerm,
				LeaderId: rf.me}
			// send heartbeats to every node
			for i := range rf.peers {
				// skip leader itself
				if i == rf.me {
					continue
				}
				// init a new goroutine to send AppendEntries
				go func(i int) {
					reply := AppendEntriesReply{}
					if ok := rf.peers[i].Call("Raft.AppendEntries", args, &reply); ok {
						rf.checkTerm(reply.Term)
					}
				}(i)
			}
			// wait until heartbeats timeout or state changed
			select {
			case <-time.After(100 * time.Millisecond):
			case <-rf.stateChanged:
			}
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// initialize Raft struct
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = STATE_FOLLOWER
	rf.heartBeat = make(chan bool)
	rf.stateChanged = make(chan bool)
	// randomly generate elecTimeOut between 200ms and 400ms
	rand.Seed(time.Now().UnixNano())
	rf.elecTimeOut = time.Duration(200 * (1 + rand.Float64())) * time.Millisecond

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// run Raft main thread
	go rf.run()

	return rf
}
