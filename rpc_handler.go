package casual_raft

type AppendEntriesRequest struct {
	Term 	   uint32     // leader's term
	LeaderID   uint32     // leader's ID
	PrevLogIndex uint32   // index of log entry immediately preceding new ones
	PrevLogTerm  uint32   // term of prevLogIndex entry
	Entries    []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit uint32   // leader's commitIndex
}

type AppendEntriesResponse struct {
	Term uint32  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

type RequestVoteRequest struct {
	Term         uint32 // candidate's term
	CandidateID uint32  // candidate requesting votes
	LastLogIndex uint32 // index of candidate's last log entry
	LastLogTerm uint32  // term of candidate's last log entry
}

type RequestVoteResponse struct {
	Term        uint32 // currentTerm, for candidate to update itself
	VoteGranted bool   // true means candidate received vote
}

type raftClient struct {
	// peers represent the list of peer addresses, e.g ["localhost:8001", "localhost:8002]
	peers []string
}

func (c *raftClient) sendAppendEntries(serverID uint32, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	return nil, nil
}

func (c *raftClient) sendRequestVote(serverID uint32, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	return nil, nil
}