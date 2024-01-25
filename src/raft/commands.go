package raft

// AppendEntriesArgs is Args for AppendEntriesRPC
type AppendEntriesArgs struct {
	Term     int // Leader's term
	LeaderId int // Leader's index in peers (Leader.me)

	PrevLogIndex int // Index of the LogEntry before the one Leader going to replicate
	PrevLogTerm  int // term of the log at prevLogIndex

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

type RequestVoteReply struct {
	VoterId     int  // who give the vote
	Term        int  // Follower's current term
	VoteGranted bool // if Candidate receive the vote
}

// !!Deprecated RequestVoteArgsFromRaft generate RequestVoteArgs from Raft state
func RequestVoteArgsFromRaft(rf *Raft) RequestVoteArgs {

	lastLogIndex, lastLogTerm := rf.getLastEntry()

	return RequestVoteArgs{
		Term:         rf.getCurrentTerm(),
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
}
