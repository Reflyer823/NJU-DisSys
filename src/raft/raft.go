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
	"fmt"
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

// 
// state constants for Raft servers
// 
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
	applyCh   chan ApplyMsg

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
	sendAgain    []chan bool
	heartBeat    chan bool
	elecTimeOut  time.Duration
	sumVotes     int
	stateChanged chan bool
}

type LogEntry struct {
	Term     int
	Command  interface{}
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
// send ApplyMsg
// 
func (rf *Raft) sendApplyMsg(index int, command interface{}) {
	msg := ApplyMsg{Index: index, Command: command}
	rf.applyCh <- msg
	fmt.Printf("%v: apply [%v, %v]\n", rf.me, index, command)
}

//
// update new CommitIndex, need mutex lock first
// 
func (rf *Raft) updateCommitIndex(newCommitIndex int) {
	// fmt.Printf("Raft %v update commitIndex = %v\n", rf.me, newCommitIndex)
	prevCommitIndex := rf.commitIndex
	rf.commitIndex = newCommitIndex
	// apply new commited logs
	for i := prevCommitIndex + 1; i <= rf.commitIndex; i++ {
		rf.sendApplyMsg(i, rf.log[i - 1].Command)
	}
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
		LastLogIndex := len(rf.log)
		LastLogTerm := 0
		if LastLogIndex > 0 {
			LastLogTerm = rf.log[LastLogIndex - 1].Term
		}
		// fmt.Printf("%v: Receive vote request from %v [%v, %v]", rf.me, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
		// fmt.Printf(" self [%v, %v]\n", LastLogIndex, LastLogTerm)
		if args.LastLogTerm > LastLogTerm || (args.LastLogTerm == LastLogTerm && args.LastLogIndex >= LastLogIndex) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			// fmt.Printf("%v: voted for %v\n", rf.me, args.CandidateId)
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
func (rf *Raft) sendRequestVote(i int, args RequestVoteArgs) {
	reply := RequestVoteReply{}
	if ok := rf.peers[i].Call("Raft.RequestVote", args, &reply); ok {
		rf.checkTerm(reply.Term)
		if reply.VoteGranted && rf.state == STATE_CANDIDATE &&
		rf.currentTerm == args.Term {
			rf.mu.Lock()
			rf.sumVotes++
			if rf.sumVotes > len(rf.peers) / 2 {
				rf.state = STATE_LEADER
				// create and initialize nextIndex array
				rf.nextIndex = make([]int, len(rf.peers))
				nextIndex := len(rf.log) + 1
				for j := range rf.nextIndex {
					rf.nextIndex[j] = nextIndex
				}
				// create and initialize matchIndex array
				rf.matchIndex = make([]int, len(rf.peers))
				for j := range rf.matchIndex {
					rf.matchIndex[j] = 0
				}
				// create and initialize success array
				rf.sendAgain = make([]chan bool, len(rf.peers))
				for j := range rf.sendAgain {
					rf.sendAgain[j] = make(chan bool)
				}
				rf.stateChanged <- true
				fmt.Printf("Raft %v becomes leader, current term: %v\n", rf.me, rf.currentTerm)
			}
			rf.mu.Unlock()
		}
	}
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
	term := rf.currentTerm
	isLeader := (rf.state == STATE_LEADER)

	if isLeader {
		rf.mu.Lock()
		rf.log = append(rf.log, LogEntry{rf.currentTerm, command})
		index = len(rf.log)
		rf.matchIndex[rf.me] = index
		rf.mu.Unlock()
		fmt.Printf("%v: append [%v, %v]\n", rf.me, index, command)
		// rf.sendApplyMsg(index, command)
	}

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
	reply.Term = rf.currentTerm
	reply.Success = false
	if rf.state == STATE_FOLLOWER {
		rf.heartBeat <- true
	}
	if rf.state == STATE_CANDIDATE && args.Term == rf.currentTerm {
		rf.mu.Lock()
		rf.state = STATE_FOLLOWER	// convert to follower
		rf.mu.Unlock()
		rf.stateChanged <- true
	}
	if args.Term < rf.currentTerm {
		return
	}
	// check previous log's index and term
	if args.PrevLogIndex > len(rf.log) {
		return
	}
	PrevLogTerm := 0
	if args.PrevLogIndex > 0 {
		PrevLogTerm = rf.log[args.PrevLogIndex - 1].Term
	}
	if PrevLogTerm != args.PrevLogTerm {
		return
	}
	reply.Success = true
	if args.Entries != nil {
		rf.mu.Lock()
		rf.log = rf.log[:args.PrevLogIndex]
		for _, v := range args.Entries {
			rf.log = append(rf.log, v)
			fmt.Printf("%v: append [%v, %v]\n", rf.me, len(rf.log), v.Command)
		}
		rf.mu.Unlock()
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.mu.Lock()
		// update commitIndex to min(leaderCommit, lastLogIndex)
		if args.LeaderCommit < len(rf.log) {
			rf.updateCommitIndex(args.LeaderCommit)
		} else {
			rf.updateCommitIndex(len(rf.log))
		}
		rf.mu.Unlock()
	}
}

// 
// send AppendEntries RPC to server i
// 
func (rf *Raft) sendAppendEntries(i int) {
	numToSend := 0
	args := AppendEntriesArgs{
		Term: rf.currentTerm,
		LeaderId: rf.me,
		LeaderCommit: rf.commitIndex}
	rf.mu.Lock()
	// fmt.Printf("%v: sendAppendEntries to %v - %v [%v, %v]\n", rf.me, i, rf.currentTerm, rf.nextIndex[i] - 1, len(rf.log))
	if rf.nextIndex[i] > 1 {
		args.PrevLogIndex = rf.nextIndex[i] - 1
		args.PrevLogTerm = rf.log[args.PrevLogIndex - 1].Term
	}
	// if have entries to send, add them to Entries array
	if len(rf.log) >= rf.nextIndex[i] {
		numToSend = len(rf.log) - rf.nextIndex[i] + 1
		args.Entries = make([]LogEntry, numToSend)
		for j := 0; j < numToSend; j++ {
			args.Entries[j] = rf.log[rf.nextIndex[i] - 1 + j]
		}
	}
	rf.mu.Unlock()
	reply := AppendEntriesReply{}
	if ok := rf.peers[i].Call("Raft.AppendEntries", args, &reply); ok {
		rf.checkTerm(reply.Term)
		if rf.state != STATE_LEADER {
			return
		}
		if reply.Success {
			if numToSend > 0 {
				rf.mu.Lock()
				rf.nextIndex[i] += numToSend
				rf.matchIndex[i] = rf.nextIndex[i] - 1
				// fmt.Printf("%v: update nextIndex[%v] = %v\n", rf.me, i, rf.nextIndex[i])
				if rf.matchIndex[i] > rf.commitIndex {
					sum := 0
					for j := range rf.matchIndex {
						if rf.matchIndex[j] >= rf.matchIndex[i] {
							sum++
						}
					}
					if sum > len(rf.peers) / 2 && rf.log[rf.matchIndex[i] - 1].Term == rf.currentTerm {
						// rf.commitIndex = rf.matchIndex[i]
						rf.updateCommitIndex(rf.matchIndex[i])
					}
				}
				rf.mu.Unlock()
			}
			// fmt.Printf("%v: send AppendEntries to %v succeeded.\n", rf.me, i)
		} else {
			// reply failed, decrease nextIndex and send again.
			rf.mu.Lock()
			rf.nextIndex[i]--
			// fmt.Printf("%v: update nextIndex[%v] = %v\n", rf.me, i, rf.nextIndex[i])
			rf.mu.Unlock()
			// fmt.Printf("%v: send AppendEntries to %v failed. Restarting...", rf.me, i)
			rf.sendAgain[i] <- true
		}
	} else {
		// fmt.Printf("%v: send request to %v failed.\n", rf.me, i)
	}
}

// 
// Raft server main thread
// 
func (rf *Raft) run() {
	for {
		switch rf.state {
		case STATE_FOLLOWER:  // follower
			timer := time.NewTimer(rf.elecTimeOut)
			select {
			case <-rf.heartBeat:
				// stop the timer
				if !timer.Stop() {
					<-timer.C
				}
			case <-timer.C:
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
			// fmt.Printf("%v: start election, sumVotes=%v\n", rf.me, rf.sumVotes)
			timer := time.NewTimer(rf.elecTimeOut)
			args := RequestVoteArgs{
				Term: rf.currentTerm,
				CandidateId: rf.me,
				LastLogIndex: len(rf.log)}
			if args.LastLogIndex > 0 {
				args.LastLogTerm = rf.log[args.LastLogIndex - 1].Term
			}
			// send RequestVote RPC to every node
			for i := range rf.peers {
				// skip candidate itself
				if i == rf.me {
					continue
				}
				// init a new goroutine to send RequestVote
				go rf.sendRequestVote(i, args)
			}
			// wait until state changed or election timeout
			select {
			case <-rf.stateChanged:
				// stop the timer
				if !timer.Stop() {
					<-timer.C
				}
			case <-timer.C:
				// increase term and start a new election
				rf.mu.Lock()
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.sumVotes = 1
				rf.mu.Unlock()
			}
			// fmt.Printf("Raft %v election ends with %v support\n", rf.me, rf.sumVotes)
		case STATE_LEADER:  // leader
			// init heartBeat thread for every node
			for i := range rf.peers {
				// skip leader itself
				if i == rf.me {
					continue
				}
				go func(i int) {
					for {
						// init a new goroutine to send AppendEntries
						go rf.sendAppendEntries(i)
						// set up a timer
						timer := time.NewTimer(100 * time.Millisecond)
						// wait until timer fired or sendAgain received
						select {
						case <-timer.C:
						case x := <-rf.sendAgain[i]:
							// stop the timer
							if !timer.Stop() {
								<-timer.C
							}
							if !x {
								return
							}
						}
					}
				}(i)
			}
			// wait until state changed
			<-rf.stateChanged
			// call all the heartBeat threads to exit
			for i := range rf.sendAgain {
				// skip leader itself
				if i == rf.me {
					continue
				}
				rf.sendAgain[i] <- false
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
	rf.applyCh = applyCh

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
