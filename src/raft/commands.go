package raft

// AppendEntriesArgs is Args for AppendEntriesRPC
type AppendEntriesArgs struct {
	Term     int // Leader's term
	LeaderId int // Leader's index in peers (Leader.me)

	// these two parameter are used to find the consistencies LogEntry between Leader and Follower
	PrevLogIndex int // Index of the LogEntry before the LogEntry Leader going to replicate
	PrevLogTerm  int // term of prevLogIndex

	Entries []*LogEntry // LogEntry to be replicated

	LeaderCommit int // Leader Commit Index
}

type AppendEntriesReply struct {
	Term int // Follower's current term (for Leader to update itself)

	Success bool // if AppendEntries Request had succeed

	LastLog int // last log help leader quick find conflict and check if Leader and Follower are in consist
}

type RequestVoteArgs struct {
	Term        int // Candidate term
	CandidateId int // Candidate Index in peers

	LastLogIndex int // Index of Candidate's last log
	LastLogTerm  int // Term of Candidate's last log
}

// RequestVoteArgsFromRaft generate RequestVoteArgs from Raft state
func RequestVoteArgsFromRaft(rf *Raft) RequestVoteArgs {

	lastLogIndex, lastLogTerm := rf.LastEntry()

	return RequestVoteArgs{
		Term:         rf.getCurrentTerm(),
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
}

type RequestVoteReply struct {
	VoterId     int  // who give the vote
	Term        int  // Follower's current term
	VoteGranted bool // if Candidate receive the vote
}
