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
	"bytes"
	"fmt"
	"sort"

	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

type State uint32

const (
	Follower State = iota
	Candidate
	Leader
	Shutdown
)

func (r State) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Shutdown:
		return "Shutdown"
	default:
		return "UnKnown"
	}
}

const (
	ElectionTimeout = 200 * time.Millisecond // limit of Timeout for election and re-election

	HeartBeatTimeout = 300 * time.Millisecond // Timeout when Follower didn't contact with a Leader

	// ReplicationTimeout for we spilt heartbeat and logReplication we need a timeout to trigger LogReplication
	// when no Start() called
	ReplicationTimeout = 100 * time.Millisecond

	RPCTimeout = 100 * time.Millisecond // timeout for an RPC call
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
	term       atomic.Value // Term of Raft cluster Node
	state      State        // Raft State(Follower,Candidate,Leader)
	votedFor   atomic.Value // the index of Candidate which is voted
	logEntries *LogStore    // LogEntries
	//applyNotifyCh chan struct{} // use to notify when log need to apply

	//2B
	lastAppliedIndex atomic.Value //  index of highest logEntry applied to state machine
	commitIndex      atomic.Value // index of highest logEntry committed

	LeaderLock       sync.Mutex
	leaderStartIndex int           // the first index Leader will use
	nextIndexes      map[int]int   // next index Leader will send to each follower
	matchIndexes     map[int]int   // index of highest log entry known to be replicated
	leaderStepDownCh chan struct{} // notify leader to step down
	logTriggerCh     chan struct{} // notify leader to start replication when Start() are called

	applyCh chan ApplyMsg // use to submit newly committed log to tester
	//2C

	lastLock sync.Mutex // protect lastLogX and lastSnapShotX
	//2D
	lastSnapshotIndex int
	lastSnapshotTerm  int

	lastLogIndex int // the index of the last log in logStore
	lastLogTerm  int // the term of the last log in logStore

	// for heartbeatTimeout
	lastContact      time.Time // last contact with Leader
	lastContactRLock sync.RWMutex

	startLock sync.Mutex
}

func (rf *Raft) String() string {
	lastIndex, _ := rf.getLastEntry()
	return fmt.Sprintf("Raft[%d] Term: %d role: %s lastIndex:%d commitIndex:%d logs:%v",
		rf.me, rf.getCurrentTerm(), rf.getRaftState(), lastIndex, rf.getCommitIndex(), rf.logEntries)
}

func (rf *Raft) getVotedFor() int {
	return parseAtomicValueInt(rf.votedFor)
}

func (rf *Raft) setVotedFor(i int) {
	rf.votedFor.Store(i)
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
	return parseAtomicValueInt(rf.term)
}

func (rf *Raft) setCurrentTerm(term int) {
	rf.term.Store(term)
}

// get raft state Candidate, Shutdown, Follower, Leader
func (rf *Raft) getRaftState() State {
	stateAddr := (*uint32)(&rf.state)
	return State(atomic.LoadUint32(stateAddr))
}

// set raft state Candidate, Shutdown, Follower, Leader
func (rf *Raft) setRaftState(s State) {
	stateAddr := (*uint32)(&rf.state)
	atomic.StoreUint32(stateAddr, uint32(s))
}

func (rf *Raft) getCommitIndex() int {
	return parseAtomicValueInt(rf.commitIndex)
}

func (rf *Raft) setCommitIndex(index int) {
	rf.commitIndex.Store(index)
}

func (rf *Raft) getLastApplied() int {
	return parseAtomicValueInt(rf.lastAppliedIndex)
}

func (rf *Raft) setLastApplied(index int) {
	rf.lastAppliedIndex.Store(index)
}

func (rf *Raft) setLastLog(index, term int) {
	rf.lastLock.Lock()
	defer rf.lastLock.Unlock()

	rf.lastLogIndex = index
	rf.lastLogTerm = term
}

func (rf *Raft) getLastLog() (index, term int) {
	rf.lastLock.Lock()
	defer rf.lastLock.Unlock()

	index = rf.lastLogIndex
	term = rf.lastLogTerm
	return
}

func (rf *Raft) setLastSnapshot(index, term int) {
	rf.lastLock.Lock()
	defer rf.lastLock.Unlock()

	rf.lastSnapshotIndex = index
	rf.lastSnapshotTerm = term
}

func (rf *Raft) getLastSnapshot() (index, term int) {
	rf.lastLock.Lock()
	defer rf.lastLock.Unlock()

	index = rf.lastSnapshotIndex
	term = rf.lastSnapshotTerm
	return
}

func (rf *Raft) LastIndex() int {
	rf.lastLock.Lock()
	defer rf.lastLock.Unlock()

	return max(rf.lastLogIndex, rf.lastSnapshotIndex)
}

// getLastEntry return the last log's index and term
func (rf *Raft) getLastEntry() (index, term int) {
	rf.lastLock.Lock()
	defer rf.lastLock.Unlock()
	index = rf.lastSnapshotIndex
	term = rf.lastSnapshotTerm
	if rf.lastLogIndex >= rf.lastSnapshotIndex {
		index = rf.lastLogIndex
		term = rf.lastLogTerm
		return
	}

	return
}

// InitLeaderState handy initialize Leader State of a raft server (nextIndexes,matchIndexes)
func (rf *Raft) InitLeaderState() {
	rf.LeaderLock.Lock()
	defer rf.LeaderLock.Unlock()
	rf.matchIndexes = make(map[int]int, len(rf.peers)-1)
	rf.nextIndexes = make(map[int]int, len(rf.peers)-1)
	rf.logTriggerCh = make(chan struct{}, 1)
	rf.leaderStepDownCh = make(chan struct{}, 1)
	lastIndex, _ := rf.getLastLog()
	rf.leaderStartIndex = lastIndex + 1

	serverCount := len(rf.peers)
	lastLogIndex, _ := rf.logEntries.LastIndex()
	for i := 0; i < serverCount; i++ {
		if i == rf.me {
			continue
		}
		rf.matchIndexes[i] = 0               // init match 0, increase
		rf.nextIndexes[i] = lastLogIndex + 1 // initialized to leader last log index + 1
	}
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (term int, isLeader bool) {
	term = rf.getCurrentTerm()
	isLeader = rf.getRaftState() == Leader
	return
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
	//start := time.Now()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.getCurrentTerm())
	e.Encode(rf.getVotedFor())
	lastSnapIndex, lastSnapTerm := rf.getLastSnapshot()
	e.Encode(lastSnapIndex)
	e.Encode(lastSnapTerm)
	e.Encode(rf.logEntries.unwrapLogs())
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	raftState := w.Bytes()
	rf.persister.Save(raftState, nil)
	//fmt.Println(time.Since(start))
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var voteFor int
	var lastSnapIndex int
	var lastSnapTerm int
	var logs []LogEntry

	var fails uint32
	if err := d.Decode(&term); err != nil {
		DPrintf("Raft[%d] failed to decode currentTerm: %v", rf.me, err)
		return
	}
	if err := d.Decode(&voteFor); err != nil {
		DPrintf("Raft[%d] failed to decode voteFor: %v", rf.me, err)
		return
	}
	if err := d.Decode(&lastSnapIndex); err != nil {
		DPrintf("Raft[%d] failed to decode lastSnapIndex: %v", rf.me, err)
		return
	}
	if err := d.Decode(&lastSnapTerm); err != nil {
		DPrintf("Raft[%d] failed to decode lastSnapTerm: %v", rf.me, err)
		return
	}
	if err := d.Decode(&logs); err != nil {
		DPrintf("Raft[%d] failed to decode logs: %v", rf.me, err)
		return
	}

	if fails > 0 {
		return
	}

	rf.setCurrentTerm(term)
	rf.setVotedFor(voteFor)
	rf.setLastSnapshot(lastSnapIndex, lastSnapTerm)
	rf.logEntries = newLogStore()
	if err := rf.logEntries.setLogs(logs); err != nil {
		DPrintf("[term %d]: Raft[%d] unable to restore log to logStore:%v", rf.getCurrentTerm(), rf.me, err)
		panic(err)
	}
	// we should update last index if recover any logs
	if logLen := len(logs); logLen > 0 {
		lastLog := logs[logLen-1]
		rf.setLastLog(lastLog.Index, lastLog.Term)
	}
	DPrintf("[term %d]: Raft[%d] recover succeed State: %v", rf.getCurrentTerm(), rf.me, rf)

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
	//DPrintf("[term %d]: Raft[%d] receive requestVote from Raft[%d]", rf.getCurrentTerm(), rf.me, args.CandidateId)

	lastlogIndex, lastlogTerm := rf.getLastEntry()

	reply.Term = rf.getCurrentTerm()
	reply.VoteGranted = false

	if rf.getCurrentTerm() > args.Term { // local are newer,reject
		return
	}

	if rf.getCurrentTerm() < args.Term { // local are older, become follower and update its term
		if rf.getRaftState() == Leader {
			rf.leaderStepDownCh <- struct{}{}
		}
		rf.setRaftState(Follower)
		rf.setCurrentTerm(args.Term)
		rf.setVotedFor(None)
		reply.Term = args.Term
		rf.persist()
	}

	// term equal
	// Grant if rf.votedFor is null or Candidate id, and candidate's log is at least as up-to-date as receiver's log
	if rf.getVotedFor() != None {
		return // we have voted in this term
	}

	if args.LastLogTerm < lastlogTerm {
		DPrintf("[term %d]: Raft[%d] reject vote request since we have greater lastLogTerm, candidate %d lastTerm %d candidate's lastTerm %d",
			rf.getCurrentTerm(), rf.me, args.CandidateId, lastlogTerm, args.LastLogTerm)
		return
	}

	if args.LastLogTerm == lastlogTerm && args.LastLogIndex < lastlogIndex {
		DPrintf("[term %d]: Raft[%d] reject vote request since we have greater lastLogIndex, candidate %d lastIndex %d candidate's lastIndex %d",
			rf.getCurrentTerm(), rf.me, args.CandidateId, lastlogIndex, args.LastLogIndex)
		return
	}

	// persist needed here
	rf.setLastContact()
	reply.VoteGranted = true
	rf.setVotedFor(args.CandidateId)
	rf.persist()
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// electSelf will vote for self and send Vote Request to other Nodes
// it will return a channel which collect RequestVoteReply
func (rf *Raft) electSelf() <-chan *RequestVoteReply {
	rf.setVotedFor(rf.me)                                   // vote for self
	voteChan := make(chan *RequestVoteReply, len(rf.peers)) // create vote collecting chan
	lastLogIndex, lastLogTerm := rf.getLastEntry()
	voteRequest := &RequestVoteArgs{
		Term:         rf.getCurrentTerm(),
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}

		go func(pidx int) {
			voteReply := new(RequestVoteReply)
			ok := rf.sendRequestVote(pidx, voteRequest, voteReply)
			if !ok {
				DPrintf("[term %d]: Raft[%d] failed to requestVote from Raft[%d]", rf.getCurrentTerm(), rf.me, pidx)
			}
			voteChan <- voteReply
		}(idx)
	}

	return voteChan
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Success = false
	reply.LastLog, _ = rf.getLastLog()
	reply.Term = rf.getCurrentTerm()

	// Ignore an older term
	if args.Term < rf.getCurrentTerm() {
		return
	}

	if args.Term > rf.getCurrentTerm() {
		if rf.getRaftState() == Leader {
			rf.leaderStepDownCh <- struct{}{} // notify step down if we are Leader
		}
		rf.setCurrentTerm(args.Term)
		rf.setRaftState(Follower)
		rf.setVotedFor(None) // come to new term clear votedFor
		rf.persist()
	}

	// check log
	if args.PrevLogIndex > 0 {
		lastIndex, lastTerm := rf.getLastLog()

		//DPrintf("[term %d]: Raft[%d] lastIndex:%d Term:%d, Args.prevIndex:%d %d", rf.getCurrentTerm(), rf.me, lastIndex, lastTerm, args.PrevLogIndex, args.PrevLogTerm)

		var prevLogTerm int
		if args.PrevLogIndex == lastIndex {
			prevLogTerm = lastTerm // leader and follower's log are match
		} else {
			prevLog := new(LogEntry)
			if err := rf.logEntries.getLog(args.PrevLogIndex, prevLog); err != nil {
				DPrintf("[term %d]: Raft[%d] in PrevLog %v,lastLog %v :%v",
					rf.getCurrentTerm(), rf.me, args.PrevLogIndex, lastIndex, err)
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

	// process logs
	if len(args.Entries) > 0 {
		lastlog, _ := rf.getLastLog()
		var newEntries []LogEntry // the logEntries to be appended to Raft
		// delete conflict part & skip repeat part
		for i, log := range args.Entries {
			if log.Index > lastlog { // all entries are new
				newEntries = args.Entries[i:]
				break
			}

			ilog := new(LogEntry)
			if err := rf.logEntries.getLog(log.Index, ilog); err != nil {
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
			last := newEntries[ln-1]
			rf.setLastLog(last.Index, last.Term)
			rf.persist()
		}
	}

	// check commit
	if args.LeaderCommit > 0 && args.LeaderCommit > rf.getCommitIndex() {
		index := min(args.LeaderCommit, rf.LastIndex())
		rf.setCommitIndex(index)
		rf.applyLogs() // apply committed log to services
	}

	reply.Success = true
	rf.setLastContact()
}

// long run routine to send heartbeat
func (rf *Raft) heartBeat(raftId int, stopCh chan struct{}) {
	//var fails uint32
	hbReq := &AppendEntriesArgs{
		Term:     rf.getCurrentTerm(),
		LeaderId: rf.me,
	}

	hbReply := new(AppendEntriesReply)
	for {
		select {
		case <-randomTimeout(HeartBeatTimeout / 3):
		case <-stopCh:
			return
		}
		if ok := rf.peers[raftId].Call("Raft.AppendEntries", hbReq, hbReply); !ok {
			DPrintf("[term %d]: Raft[%d] failed to heartBeat to Raft[%d]", rf.getCurrentTerm(), rf.me, raftId)
			//nextRetry := calculateRetryTime(10*time.Microsecond, fails, 10, HeartBeatTimeout/2)
			//fails++
			//select {
			//case <-time.After(nextRetry):
			//case <-stopCh:
			//	return
			//}
		} else {
			rf.setLastContact()
			//fails = 0
		}
	}
}

// LeaderReplicateTo is goroutine that copy logEntry from Leader to one Follower
// leaderStepDownCh are used to notify Leader to stepDown when new term are discovered,
func (rf *Raft) LeaderReplicateTo(raftId int, leaderStepDownCh chan struct{}, stopCh chan struct{}) {
	rf.LeaderLock.Lock()
	if rf.getRaftState() != Leader { // we are facing some issue that Leader step down after the replication started
		rf.LeaderLock.Unlock()
		return
	}
	nextIndex := rf.nextIndexes[raftId]
	rf.LeaderLock.Unlock()
	lastLogIndex, _ := rf.getLastLog()

SendLog:
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
		args.PrevLogIndex = l.Index
		args.PrevLogTerm = l.Term
	}
	// append log into args.Entries
	logLen := min(nextIndex+MaxBufferCh-1, lastLogIndex)
	args.Entries = make([]LogEntry, 0)
	for i := nextIndex; i <= logLen; i++ {
		tmpLog := new(LogEntry)
		if err := rf.logEntries.getLog(i, tmpLog); err != nil {
			DPrintf("Raft[%v] failed to get log at %d: %v", rf.me, nextIndex-1, err)
			return
		}
		args.Entries = append(args.Entries, *tmpLog)
	}

	reply := new(AppendEntriesReply)

	rf.peers[raftId].Call("Raft.AppendEntries", args, reply)

	leaderTerm := rf.getCurrentTerm()
	if reply.Term > leaderTerm {
		DPrintf("[term %d]: Raft[%d] Discover new [term %d] when appendEntries", leaderTerm, rf.me, reply.Term)
		leaderStepDownCh <- struct{}{} //
	}

	rf.setLastContact()

	rf.LeaderLock.Lock()             // Atomic transaction: change nextIndex and matchIndex
	if rf.getRaftState() != Leader { // we will check if we are Leader before we change Leader state
		rf.LeaderLock.Unlock()
		return
	}

	if reply.Success {
		if logs := args.Entries; len(logs) > 0 {
			last := logs[len(logs)-1]
			// update matchIndex & nextIndex increase
			rf.matchIndexes[raftId] = last.Index
			rf.nextIndexes[raftId] = last.Index + 1
			// try to commit log after log replication
			rf.leaderCommit()
		}
	} else {
		//update nextIndex: decrease
		newIndex := max(min(rf.nextIndexes[raftId]-1, reply.LastLog+1), 1)
		rf.nextIndexes[raftId] = newIndex
	}
	rf.LeaderLock.Unlock()

	select {
	case <-stopCh:
		return
	default:
	}
	// check if we should continue sending log to help Follower quickly catch up
	rf.LeaderLock.Lock()
	nextIndex = rf.nextIndexes[raftId]
	rf.LeaderLock.Unlock()
	if nextIndex <= lastLogIndex {
		goto SendLog
	}
	return

	// send snapshot
}

// Call by Leader to update commitIndex after replicate log to peer
// call with lock held
func (rf *Raft) leaderCommit() {
	if len(rf.matchIndexes) < 0 {
		return
	}

	t := make([]int, 0)
	for _, nMatchIndex := range rf.matchIndexes {
		t = append(t, nMatchIndex)
	}
	sort.Sort(intSlice4Sort(t))
	majorityMatch := t[(len(rf.peers))/2] // chose the middle of sorted slice

	if majorityMatch > rf.getCommitIndex() && majorityMatch >= rf.leaderStartIndex {
		rf.setCommitIndex(majorityMatch)
		//DPrintf("[term %d]: Raft[%d] update commit Index %d", rf.getCurrentTerm(), rf.me, majorityMatch)
		//rf.applyLogs()
	}
	//lastLogIndex, _ := rf.getLastLog()
	//shouldCommit := false
	//
	//for i := rf.getCommitIndex() + 1; i <= lastLogIndex; i++ {
	//	consensusCount := 1 // Leader are consensus
	//	for _, m := range rf.matchIndexes {
	//		if m >= i {
	//			consensusCount += 1
	//			if consensusCount*2 > len(rf.peers) {
	//				rf.setCommitIndex(i)
	//				shouldCommit = true
	//				break
	//			}
	//		}
	//	}
	//	if rf.getCommitIndex() != i {
	//		break
	//	}
	//}
	//
	//if shouldCommit {
	//	rf.applyLogs(rf.getCommitIndex())
	//}
}

func (rf *Raft) applyRoutine(shutdownCh chan struct{}) {
	applyTimer := time.After(ReplicationTimeout / 5)
	for {
		select {
		case <-shutdownCh:
			return
		case <-applyTimer:
			applyTimer = time.After(ReplicationTimeout / 5)
			rf.applyLogs()
		}
	}
}

// applyLogs will apply all the committed & not applied log to services(in lab is applyCh)
// for lab tester's competition log will transmit this way (logs->bufferCh->applyCh)
func (rf *Raft) applyLogs() {

	index := rf.getCommitIndex()
	lastApplied := rf.getLastApplied()
	if index <= lastApplied {
		//DPrintf("[term %d]: Raft[%d] skipping old log application %d, lastApplied %d", rf.getCurrentTerm(), rf.me, index, lastApplied)
		return
	}
	// we set a maximum number of submissions at one time to prevent re-election caused by
	// the Leader trying to apply a large number of msg
	applyLen := min(index-lastApplied, MaxBufferCh)
	DPrintf("[term %d]: Raft[%d] prepare to apply until %d, %d will be applied", rf.getCurrentTerm(), rf.me, index, applyLen)

	bufferCh := make(chan ApplyMsg, applyLen) // chan to hold to-send ApplyMsg
	// send log to buffer
	for idx := lastApplied + 1; idx <= index; idx++ {
		log := new(LogEntry)
		if err := rf.logEntries.getLog(idx, log); err != nil {
			DPrintf("[term %d]: Raft[%d] error while applyLogs[%v]: %v", rf.getCurrentTerm(), rf.me, idx, err)
			return
		}
		DPrintf("[term %d]: Raft[%d] send %d:%d", rf.getCurrentTerm(), rf.me, idx, log.Command)
		msg := ApplyMsg{
			CommandValid: true,
			Command:      log.Command,
			CommandIndex: idx,
		}
		bufferCh <- msg
	}

	// goroutine{buffer->applyCh}
	go func() {
		sended := make([]int, 0)
		for {
			select {
			case msg := <-bufferCh:
				sended = append(sended, msg.CommandIndex)
				rf.applyCh <- msg
			default:
				DPrintf("Raft[%d] apply %v to tester", rf.me, sended)
				return
			}
		}
	}()

	// update index
	rf.setLastApplied(rf.getLastApplied() + applyLen)
}

// LeaderReplication handy start replication to each follower
func (rf *Raft) LeaderReplication(stopCh chan struct{}) {
	for id := 0; id < len(rf.peers); id++ {
		if id == rf.me {
			continue
		}
		go rf.LeaderReplicateTo(id, rf.leaderStepDownCh, stopCh)
	}
	//rf.leaderCommit() // another type of timing trigger XD
}

func (rf *Raft) runCandidate() {

	rf.setCurrentTerm(rf.getCurrentTerm() + 1) // Increase self term

	DPrintf("[term %d]: Raft[%d] enter Candidate state", rf.getCurrentTerm(), rf.me)

	electionTimer := randomTimeout(ElectionTimeout)

	voteCh := rf.electSelf()

	voteNeeded := rf.majoritySize()
	votes := 1

	for rf.getRaftState() == Candidate {
		select {
		case v := <-voteCh:
			if v.Term > rf.getCurrentTerm() {
				DPrintf("[term %d]: Raft[%d] New Term discovered %v , fallback to follower", rf.getCurrentTerm(), rf.me, v.Term)
				rf.setRaftState(Follower)
				rf.setCurrentTerm(v.Term)
				return
			}

			if v.VoteGranted {
				votes++
			}

			if votes >= voteNeeded { // received the majority of vote and become a Leader
				DPrintf("[term %d]: Raft[%d] Win! %d/%d", rf.getCurrentTerm(), rf.me, votes, len(rf.peers))
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
	DPrintf("[term %d]: Raft[%d] enter Follower state", rf.getCurrentTerm(), rf.me)
	heartbeatTimer := randomTimeout(HeartBeatTimeout)

	for rf.getRaftState() == Follower {
		select {
		case <-heartbeatTimer:
			heartbeatTimer = randomTimeout(HeartBeatTimeout)
			// if any success contact happened in HeartBeatTimeout
			lastContact := rf.getLastContact()
			if time.Since(lastContact) < HeartBeatTimeout {
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
	DPrintf("[term %d]: Raft[%d] enter Leader state", rf.getCurrentTerm(), rf.me)

	// we heartbeat to each Raft Node in goroutine instead of from main leader loop
	stopChan := make(chan struct{}, 1)
	for id := 0; id < len(rf.peers); id++ {
		if id == rf.me {
			continue
		}
		go rf.heartBeat(id, stopChan)
	}

	go rf.applyRoutine(stopChan)

	rf.InitLeaderState()

	defer func() { // use to do clean job after leader step down
		rf.setLastContact()
		close(stopChan)

		rf.matchIndexes = nil
		rf.nextIndexes = nil
		rf.logTriggerCh = nil
		rf.leaderStepDownCh = nil
	}()

	commitTimer := time.After(ReplicationTimeout)

	for rf.getRaftState() == Leader {
		select {
		case <-rf.leaderStepDownCh:
			rf.setRaftState(Follower)
			return
		case <-rf.logTriggerCh:
			rf.persist()
			rf.LeaderReplication(stopChan)
		case <-commitTimer:
			commitTimer = time.After(ReplicationTimeout)
			rf.setLastContact()
			rf.LeaderReplication(stopChan)
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
	rf.startLock.Lock()
	defer rf.startLock.Unlock()
	if rf.getRaftState() != Leader {
		isLeader = false
		return index, term, isLeader
	}

	lastIndex, _ := rf.getLastLog()
	index = lastIndex + 1

	log := LogEntry{
		Term:    term,
		Command: command,
		Index:   index,
	}
	err := rf.logEntries.setLog(log)
	if err != nil {
		panic(fmt.Sprintf("[term %d]: Raft[%d] failed to set log:%v", rf.getCurrentTerm(), rf.me, err))
		return index, term, isLeader
	}
	rf.setLastLog(log.Index, log.Term)
	asyncNotifyCh(rf.logTriggerCh)
	//DPrintf("[term %d]: Raft[%d] get %v", rf.getCurrentTerm(), rf.me, log)

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

	BuddhaBless(false) // we introduce Buddha to give buff to the Node

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.setRaftState(Follower)
	rf.setCurrentTerm(1)
	rf.setVotedFor(None)
	rf.setLastApplied(0)
	rf.setCommitIndex(0)

	rf.applyCh = applyCh
	rf.shutdownCh = make(chan struct{}, 1)
	rf.logEntries = newLogStore()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.mainLoop()

	return rf
}
