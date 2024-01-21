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
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	Follower  int = 0
	Candidate int = 1
	Leader    int = 2
	Shutdown  int = 3
)

const (
	ElectionTimeout  = 150 * time.Millisecond // limit of Timeout for election and AppendEntries
	HeartBeatTimeout = 100 * time.Millisecond // time limited 10 HeartBeat per second
	TimeoutOffset    = 20 * time.Millisecond  // Offset to fix issue may cause by the less Heartbeat
	RPCTimeout       = 100 * time.Millisecond // timeout for an RPC call
)

const (
	MaxBufferCh = 200
)

const None = -1

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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
	//dead      int32               // set by Kill() *Depressed

	// shutdown util
	shutdownCh chan struct{} // Notify Role goroutine to exit

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//2A
	term       int       // Term of Raft cluster Node
	state      int       // Raft State(Follower,Candidate,Leader)
	votedFor   int       // the index of Candidate which is voted
	logEntries *LogStore // LogEntries
	//applyNotifyCh chan struct{} // use to notify when log need to apply

	//2B
	lastAppliedIndex int //  index of highest logEntry applied to state machine
	commitIndex      int // index of highest logEntry committed

	LeaderLock       sync.Mutex
	nextIndexes      []int         // next index Leader will send to each follower
	matchIndexes     []int         // index of highest log entry known to be replicated
	leaderStepDownCh chan struct{} // notify leader to step down

	applyCh chan ApplyMsg // use to submit newly committed log to tester
	//2C

	//2D
	lastSnapshotIndex int
	lastSnapshotTerm  int

	lastLogIndex int // the index of the last log in logStore
	lastLogTerm  int // the term of the last log in logStore

	// for heartbeatTimeout
	lastContact      time.Time // last contact with Leader
	lastContactRLock sync.RWMutex
}

func (rf *Raft) VotedFor() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.votedFor
}

func (rf *Raft) setVotedFor(i int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.votedFor = i
}

// get the time that last contact with Leader(using ReadLock)
func (rf *Raft) getLastContact() (last time.Time) {
	rf.lastContactRLock.RLock()
	defer rf.lastContactRLock.RUnlock()

	last = rf.lastContact
	return
}

func (rf *Raft) setLastContact() {
	rf.lastContactRLock.RLock()
	defer rf.lastContactRLock.RUnlock()

	rf.lastContact = time.Now()
	return
}

func (rf *Raft) getCurrentTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.term
}

func (rf *Raft) setCurrentTerm(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.term = term
}

// get raft Role Candidate/Shutdown/Follower/Leader
func (rf *Raft) getRaftState() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

func (rf *Raft) setRaftState(state int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = state
}

func (rf *Raft) getCommitIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.commitIndex
}

func (rf *Raft) setCommitIndex(index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.commitIndex = index
}

func (rf *Raft) getLastApplied() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastAppliedIndex
}

func (rf *Raft) setLastApplied(index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.commitIndex = rf.lastAppliedIndex
}

func (rf *Raft) setLastLog(term, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastLogIndex = index
	rf.lastLogTerm = term
}

func (rf *Raft) getLastLog() (term, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index = rf.lastLogIndex
	term = rf.lastLogTerm
	return
}

func (rf *Raft) setLastSnapshot(term, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastSnapshotIndex = index
	rf.lastSnapshotTerm = term
}

func (rf *Raft) getLastSnapshot() (term, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index = rf.lastSnapshotIndex
	term = rf.lastSnapshotTerm
	return
}

func (rf *Raft) LastIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return max(rf.lastLogIndex, rf.lastSnapshotIndex)
}

// LastEntry return the last log's index and term
func (rf *Raft) LastEntry() (int, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.lastLogIndex >= rf.lastSnapshotIndex {
		return rf.lastLogIndex, rf.lastLogTerm
	}

	return rf.lastSnapshotIndex, rf.lastSnapshotTerm
}

// InitLeaderState handy initialize Leader State of a raft server (nextIndexes,matchIndexes)
func (rf *Raft) InitLeaderState() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.matchIndexes = make([]int, len(rf.peers))
	rf.nextIndexes = make([]int, len(rf.peers))

	serverCount := len(rf.peers)
	lastLogIndex := rf.lastLogIndex
	for i := 0; i < serverCount; i++ {
		rf.matchIndexes[i] = 0               // init match 0, increase
		rf.nextIndexes[i] = lastLogIndex + 1 // initialized to leader last log index + 1
	}
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.term, rf.state == Leader
}

// vote needed
func (rf *Raft) majoritySize() (majority int) {
	majority = len(rf.peers)/2 + 1
	return
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

// RequestVote handle RequestVote RPC call from other raft server.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("[term %d]: Raft[%d] receive requestVote from Raft[%d] %v", rf.getCurrentTerm(), rf.me, args.CandidateId, time.Now())

	nodeTerm := rf.getCurrentTerm()
	lastlogindex, lastlogterm := rf.LastEntry()

	reply.Term = nodeTerm
	reply.VoteGranted = false

	if nodeTerm > args.Term { // local are newer,reject
		return
	}

	if nodeTerm < args.Term { // local are older, become follower and update its term
		rf.setRaftState(Follower)
		rf.setCurrentTerm(args.Term)
		rf.setVotedFor(None)
		reply.Term = args.Term

	}

	// term equal
	// Grant if rf.votedFor is null or Candidate id, and candidate's log is at least as up-to-date as receiver's log
	if rf.votedFor == None || rf.votedFor == args.CandidateId {
		// check log, larger term or same term, larger index -> at least as up-to-date
		if args.LastLogTerm > lastlogterm || (args.LastLogTerm == lastlogterm && args.LastLogIndex >= lastlogindex) {
			reply.VoteGranted = true
			reply.VoterId = rf.me
			rf.setVotedFor(args.CandidateId)
			DPrintf("[term %d]: Raft[%d] role：%v，votedFor: %v", rf.getCurrentTerm(), rf.me, rf.getRaftState(), rf.VotedFor())
		}
	}
	rf.setLastContact()
	return
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
//func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
//	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
//	return ok
//}

// electSelf will vote for self and send Vote Request to other Nodes
// it will return a channel which collect RequestVoteReply
func (rf *Raft) electSelf() <-chan *RequestVoteReply {
	rf.setVotedFor(rf.me)                                   // vote for self
	voteChan := make(chan *RequestVoteReply, len(rf.peers)) // create vote collecting chan
	voteReq := RequestVoteArgsFromRaft(rf)
	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}

		go func(pidx int) {
			voteReply := new(RequestVoteReply)
			s := rf.peers[pidx].Call("Raft.RequestVote", &voteReq, voteReply)
			if !s {
				DPrintf("[term %d]: Raft[%d] failed to requestVote from Raft[%d]", rf.getCurrentTerm(), rf.me, pidx)
			}
			voteChan <- voteReply
		}(idx)
	}

	return voteChan
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	DPrintf("[term %d]: Raft[%d] receive appendEntries from Raft[%d] in %v", rf.getCurrentTerm(), rf.me, args.LeaderId, time.Now())

	reply.Success = false
	_, reply.LastLog = rf.getLastLog()
	reply.Term = rf.getCurrentTerm()

	// Ignore an older term
	if args.Term < rf.getCurrentTerm() {
		return
	}

	if args.Term > rf.getCurrentTerm() {
		if rf.getRaftState() == Leader {
			rf.leaderStepDownCh <- struct{}{} // notify Leader to step down
		}
		rf.setCurrentTerm(args.Term)
		rf.setRaftState(Follower)
		rf.setVotedFor(None)
	}

	// check log
	if args.PrevLogIndex > 0 {
		lastterm, lastindex := rf.getLastLog()

		var prevLogTerm int
		if args.PrevLogIndex == lastindex {
			prevLogTerm = lastterm // leader and follower's log are match
		} else {
			var prevLog LogEntry
			if err := rf.logEntries.getLog(args.PrevLogIndex, &prevLog); err == nil {
				DPrintf("[term %d]: Raft[%d] in PrevLog %v,lastLog %v :%v",
					rf.getCurrentTerm(), rf.me, args.PrevLogIndex, lastindex, err)
				return
			}
			prevLogTerm = prevLog.Term
		}

		if args.PrevLogTerm != prevLogTerm {
			DPrintf("[term %d]: Raft[%d] log term mis-match Local:%v Remote %v",
				rf.getCurrentTerm(), rf.me, prevLogTerm, args.PrevLogTerm)
			return
		}
	}

	_, lastlog := rf.getLastLog()
	// process logs
	if len(args.Entries) > 0 {
		var newEntries []*LogEntry // the logEntries to be appended to Raft
		// delete conflict part & skip repeat part
		for i, log := range args.Entries {
			if log.Index > lastlog { // all entries are new
				newEntries = args.Entries[i:]
				break
			}

			var ilog LogEntry
			if err := rf.logEntries.getLog(log.Index, &ilog); err != nil {
				DPrintf("[term %d]: Raft[%d] fail to get log in %v: %v",
					rf.getCurrentTerm(), rf.me, log.Index, err)
				return
			}

			if log.Term != ilog.Term { // conflict happened clear any log after ilog.Index in rf.logEntries
				DPrintf("[term %d]: Raft[%d] try to delete conflicting log [%d %d]",
					rf.getCurrentTerm(), rf.me, log.Index, lastlog)

				if err := rf.logEntries.DeleteRange(log.Index, lastlog); err != nil {
					DPrintf("[term %d]: Raft[%d] fail to delete log in : %v",
						rf.getCurrentTerm(), rf.me, err)
					return
				}
				newEntries = args.Entries[i:] // pass old&non-conflict log
				break
			}
		}
		// append new
		if ln := len(newEntries); ln > 0 {
			if err := rf.logEntries.setLogs(newEntries); err != nil {
				DPrintf("[term %d]: Raft[%d] failed to append log: %v",
					rf.getCurrentTerm(), rf.me, err)
				return
			}

			// update lastLog the newest log is newEntries[ln-1] last one in newEntries
			rf.setLastLog(newEntries[ln-1].Term, newEntries[ln-1].Index)
		}
	}

	// check commit
	if args.LeaderCommit > 0 && args.LeaderCommit > rf.getCommitIndex() {
		index := min(args.LeaderCommit, rf.LastIndex())
		rf.setCommitIndex(index)
		//rf.applyNotifyCh <- struct{}{}
		rf.applyLogs(index) // apply committed log to services
	}
	reply.Success = true
	rf.setLastContact()
}

// LeaderReplication is goroutine that copy logEntry from Leader to Follower
// leaderStepDownCh are used to notify Leader to stepDown when new term are discovered,
func (rf *Raft) LeaderReplication(raftId int, leaderStepDownCh chan struct{}) {
	rf.LeaderLock.Lock()
	if rf.getRaftState() != Leader { // we are facing some issue that Leader step down after the replication
		return
	}
	nextIndex := rf.nextIndexes[raftId]
	rf.LeaderLock.Unlock()
	_, lastLogIndex := rf.getLastLog()

	// setup args for appendEntries
	args := new(AppendEntriesArgs)
	args.Term = rf.getCurrentTerm()
	args.LeaderId = rf.me
	args.LeaderCommit = rf.getCommitIndex()

	// set prevLogIndex and prevLogTerm
	if nextIndex == 1 {
		args.PrevLogIndex = 0
		args.PrevLogTerm = 0
	} else {
		var l LogEntry
		err := rf.logEntries.getLog(nextIndex-1, &l)
		if err != nil {
			DPrintf("Raft[%v] failed to get log at %d: %v", rf.me, nextIndex-1, err)
			return
		}
	}
	// append log into args.Entries
	logLen := min(nextIndex+MaxBufferCh-1, lastLogIndex)
	args.Entries = make([]*LogEntry, logLen)
	for i := nextIndex; i <= logLen; i++ {
		var tmpLog *LogEntry
		if err := rf.logEntries.getLog(i, tmpLog); err != nil {
			DPrintf("Raft[%v] failed to get log at %d: %v", rf.me, nextIndex-1, err)
			return
		}
		args.Entries = append(args.Entries, tmpLog)
	}

	reply := new(AppendEntriesReply)
	rf.peers[raftId].Call("Raft.AppendEntries", args, reply)

	leaderTerm := rf.getCurrentTerm()
	if reply.Term > leaderTerm {
		DPrintf("[term %d]: Raft[%d] Discover new [term %d] when appendEntries", leaderTerm, rf.me, reply.Term)
		leaderStepDownCh <- struct{}{} //
	}

	if rf.getRaftState() != Leader { // we will check if we are Leader before we change Leader state
		return
	}
	rf.setLastContact()

	if reply.Success {
		if logs := args.Entries; len(logs) > 0 {
			last := logs[len(logs)-1]
			// update matchIndex & nextIndex increase
			rf.LeaderLock.Lock()
			rf.matchIndexes[raftId] = last.Index
			rf.nextIndexes[raftId] = last.Index + 1
		}
		// try to commit log
		rf.leaderCommit()
	} else {
		//update nextIndex decrease
		rf.LeaderLock.Lock()
		rf.nextIndexes[raftId] = max(min(rf.nextIndexes[raftId]-1, reply.LastLog+1), 1)
		rf.LeaderLock.Unlock()
	}

	// send snapshot
}

// Call by Leader to update commitIndex after replicate log to peer
func (rf *Raft) leaderCommit() {
	_, lastLogIndex := rf.getLastLog()
	shouldCommit := false

	for i := rf.getCommitIndex() + 1; i <= lastLogIndex; i++ {
		consensusCount := 0
		rf.LeaderLock.Lock()
		for _, m := range rf.matchIndexes {
			if m >= i {
				consensusCount += 1
				if consensusCount > len(rf.peers)/2 {
					rf.setCommitIndex(i)
					shouldCommit = true
					DPrintf("[term %d]: Raft[%d] update commitIndex：%d", rf.getCurrentTerm(), rf.me, i)
					break
				}
			}
		}
		rf.LeaderLock.Unlock()
		if rf.getCommitIndex() != i {
			break
		}
	}

	if shouldCommit {
		rf.applyLogs(rf.getCommitIndex())
	}
}

func (rf *Raft) runCandidate() {

	rf.setCurrentTerm(rf.getCurrentTerm() + 1) // Increase self term

	DPrintf("[term %d]: Raft[%d] entering Candidate state", rf.term, rf.me)

	electionTimer := randomTimeout(ElectionTimeout)

	voteCh := rf.electSelf()

	voteNeeded := rf.majoritySize()
	votes := 1

	for rf.getRaftState() == Candidate {
		select {
		case v := <-voteCh:
			if v.Term > rf.term {
				DPrintf("[term %d]: Raft[%d] New Term discovered %v , fallback to follower", rf.getCurrentTerm(), rf.me, v.Term)
				rf.setRaftState(Follower)
				rf.setCurrentTerm(v.Term)
				return
			}

			if v.VoteGranted {
				DPrintf("[term %d]: Raft[%d] Vote to Raft[%d]", v.Term, v.VoterId, rf.me)
				votes++
			}

			if votes >= voteNeeded { // received the majority of vote and become a Leader
				DPrintf("[term %d]: Raft[%d] WIN!! Term %v , %v/%v", rf.getCurrentTerm(), rf.me, v.Term, votes, len(rf.peers))
				rf.setRaftState(Leader)
				return
			}
		case <-electionTimer:
			DPrintf("[term %d]: Raft[%d] Election Timeout , restarting new election", rf.getCurrentTerm(), rf.me)
			return
		case <-rf.shutdownCh:
			return
		}
	}
}

func (rf *Raft) runFollower() {
	DPrintf("[term %d]: Raft[%d] entering Follower state", rf.getCurrentTerm(), rf.me)
	heartbeatTimer := randomTimeout(ElectionTimeout)

	for rf.getRaftState() == Follower {
		select {
		case <-heartbeatTimer:
			heartbeatTimer = randomTimeout(ElectionTimeout)
			// if any success contact happened in HeartBeatTimeout
			lastContact := rf.getLastContact()
			if time.Since(lastContact) < ElectionTimeout {
				continue
			}
			// Heartbeat failed! Become Candidate
			DPrintf("[term %d]: Raft[%d] ElectionTimeout! %v", rf.getCurrentTerm(), rf.me, time.Now())
			rf.setRaftState(Candidate)
			return
		case <-rf.shutdownCh:
			return
		}
	}

}

func (rf *Raft) runLeader() {
	DPrintf("[term %d]: Raft[%d] entering Leader state", rf.getCurrentTerm(), rf.me)

	rf.InitLeaderState()
	//DPrintf("[term %d]: Raft[%d] %v", rf.getCurrentTerm(), rf.me, rf.matchIndexes)
	//DPrintf("[term %d]: Raft[%d] %v", rf.getCurrentTerm(), rf.me, rf.nextIndexes)

	defer func() { // use to do clean job after leader step down
		rf.setLastContact()

		rf.matchIndexes = nil
		rf.nextIndexes = nil
	}()

	heartBeatTimer := time.After(HeartBeatTimeout)

	for rf.getRaftState() == Leader {
		select {
		case <-rf.leaderStepDownCh:
			rf.setRaftState(Follower)
			return
		case <-heartBeatTimer:
			heartBeatTimer = time.After(HeartBeatTimeout)
			rf.setLastContact()
			// send AppendEntries either for heartbeat or log replication
			for id := 0; id < len(rf.peers); id++ {
				if id == rf.me {
					continue
				}
				DPrintf("[term %d]: Raft[%d] role %d start replicate to Raft[%d]", rf.getCurrentTerm(), rf.me, rf.getRaftState(), id)
				go rf.LeaderReplication(id, rf.leaderStepDownCh)
			}

		case <-rf.shutdownCh:
			return
		}
	}
}

func (rf *Raft) mainLoop() {
	for {
		select {
		case <-rf.shutdownCh:
			DPrintf("Raft[%d] shutdown", rf.me)
			return
		default:
		}
		switch rf.getRaftState() {
		case Leader:
			rf.runLeader()
		case Follower:
			rf.runFollower()
		case Candidate:
			rf.runCandidate()
		case Shutdown:
			return
		}
	}
}

// applyLogs will apply all the committed & not applied log to services(in lab is applyCh)
// for lab tester's competition log will transmit this way (logs->bufferCh->applyCh)
func (rf *Raft) applyLogs(index int) {

	lastApplied := rf.getLastApplied()
	// we set a maximum number of submissions at one time to prevent re-election caused by
	// the Leader node trying to apply a large number of msg
	lenToApply := min(index, MaxBufferCh)

	bufferCh := make(chan ApplyMsg, lenToApply) // chan to hold to-send ApplyMsg

	// push log to buffer
	for idx := lastApplied; idx <= lenToApply; idx++ {
		var log LogEntry
		if err := rf.logEntries.getLog(idx, &log); err != nil {
			DPrintf("[term %d]: Raft[%d] error while applyLogs: %v", rf.getCurrentTerm(), rf.me, err)
			return
		}
		msg := ApplyMsg{
			CommandValid: true,
			Command:      log.Command,
			CommandIndex: idx,
		}

		bufferCh <- msg
	}

	// goroutine{buffer->applyCh}
	go func() {
		for {
			select {
			case msg := <-bufferCh:
				rf.applyCh <- msg
			default:
				return
			}
		}
	}()

	// update index
	rf.setLastApplied(lenToApply)
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
	term := rf.getCurrentTerm()
	isLeader := true

	// Your code here (2B).
	if rf.getRaftState() != Leader {
		isLeader = false
	}
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
	rf.shutdownCh <- struct{}{}
	rf.setRaftState(Shutdown)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	return rf.getRaftState() == Shutdown
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.term = 1
	rf.votedFor = None
	rf.lastAppliedIndex = 0
	rf.commitIndex = 0

	rf.applyCh = applyCh
	rf.shutdownCh = make(chan struct{}, 10)
	rf.leaderStepDownCh = make(chan struct{}, 10)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.mainLoop()

	return rf
}
