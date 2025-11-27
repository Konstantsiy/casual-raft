package server

import "errors"

func (s *Server) HandleAppendCommand(cmd []byte) error {
	s.mx.Lock()
	defer s.mx.Unlock()

	if s.state != Leader {
		return errors.New("server is not leader")
	}

	lastIndex := uint32(0)
	if len(s.persistentState.log) > 0 {
		lastIndex = s.persistentState.log[len(s.persistentState.log)-1].Index
	}

	entry := logEntry{
		Index:   lastIndex,
		Term:    s.persistentState.currentTerm,
		Command: cmd,
	}

	s.persistentState.log = append(s.persistentState.log, entry)
	_ = s.persist()

	return nil
}

func (s *Server) HandleAppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse {
	s.mx.Lock()
	defer s.mx.Unlock()

	var resp = &AppendEntriesResponse{
		Term:    s.persistentState.currentTerm,
		Success: false,
	}

	// check the relevance of the requested term
	if req.Term < s.persistentState.currentTerm {
		return resp
	}

	// update state if term is higher
	if req.Term > s.persistentState.currentTerm {
		s.persistentState.currentTerm = req.Term
		s.state = Follower
		s.persistentState.votedFor = 0
		_ = s.persist()
	}

	s.resetElectionTimer()

	// Check if logs contain entry at prevLogIndex with matching term,
	// this is needed to ensure logs are consistent
	// e.g. if leader has entries [1,2,3] and follower has [1,2,4],
	// when leader tries to append entry 5 after 3, follower must reject,
	// because its log is inconsistent (it doesn't have entry 3).
	// So follower rejects, and on the next heartbeat leader must decrement nextIndex and retry
	// that means leader will send entry 3 again.
	// Follower will see that entry 3 doesn't match its entry 4,
	// follower will delete entry 4 and append entry 3, then 5 - this way logs become consistent.
	// If logs are empty, prevLogIndex is 0 and this check is skipped.
	if req.PrevLogIndex > 0 {
		var found = false

		for _, entry := range s.persistentState.log {
			if entry.Index == req.PrevLogIndex {
				if entry.Term != req.PrevLogTerm {
					return resp
				}

				found = true
				break
			}
		}

		if !found {
			return resp
		}
	}

	// Append any new entries not already in the log
	for _, newEntry := range req.Entries {
		var found = false

		for i, entry := range s.persistentState.log {
			if entry.Index == newEntry.Index {
				if entry.Term != newEntry.Term {
					// delete all entries from this index onwards, because they are conflicted
					s.persistentState.log = s.persistentState.log[:i]
					// append the new entry
					s.persistentState.log = append(s.persistentState.log, newEntry)
				}
			}

			found = true
			break
		}

		if !found {
			s.persistentState.log = append(s.persistentState.log, newEntry)
		}
	}

	if len(req.Entries) > 0 {
		// we have new entries, persist the state
		_ = s.persist()
	}

	// update commit index
	if req.LeaderCommit > s.volatileState.commitedIndex {
		// this is the highest index known to be commited on leader
		var lastNewEntryIndex = uint32(0)

		if len(req.Entries) > 0 {
			// if we have new entries, the last one is the highest
			lastNewEntryIndex = req.Entries[len(req.Entries)-1].Index
		}

		if req.LeaderCommit > lastNewEntryIndex {
			// if leaderCommit is higher than our last new entry, set to leaderCommit,
			// e.g., when there are no new entries
			s.volatileState.commitedIndex = req.LeaderCommit
		} else if lastNewEntryIndex > 0 {
			// otherwise set to last new entry index
			s.volatileState.commitedIndex = lastNewEntryIndex
		}

		// apply commited entries to state machine
		s.applyCommitedEntries()
	}

	resp.Success = true
	return resp
}

func (s *Server) HandleRequestVote(req *RequestVoteRequest) *RequestVoteResponse {
	s.mx.Lock()
	defer s.mx.Unlock()

	var resp = &RequestVoteResponse{
		Term:        s.persistentState.currentTerm,
		VoteGranted: false,
	}

	// check the relevance of the requested term, reject if it's lower
	if req.Term < s.persistentState.currentTerm {
		return resp
	}

	// update state if term is higher
	if req.Term > s.persistentState.currentTerm {
		s.persistentState.currentTerm = req.Term
		s.state = Follower
		s.persistentState.votedFor = 0
		_ = s.persist()
	}

	// check if we've already voted in this term
	if s.persistentState.votedFor != 0 &&
		s.persistentState.votedFor != req.CandidateID {
		return resp
	}

	var lastLogIndex = uint32(0)
	var lastLogTerm = uint32(0)

	// determine our last log index and term,
	// needed for candidate log up-to-date check
	if len(s.persistentState.log) > 0 {
		var lastEntry = s.persistentState.log[len(s.persistentState.log)-1]
		lastLogIndex = lastEntry.Index
		lastLogTerm = lastEntry.Term
	}

	// check if candidate's log is at least as up to date as receiver's log
	// (section 5.4.1 of Raft thesis: https://raft.github.io/raft.pdf)
	//
	// if candidate's log is more up-to-date, grant vote, otherwise, deny vote
	var logUpToDate = req.LastLogTerm > lastLogTerm ||
		(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)

	if logUpToDate {
		// grant vote
		s.persistentState.votedFor = req.CandidateID

		if err := s.persist(); err != nil {
			// don't grant a vote if server can't persist
			return resp
		}

		s.resetElectionTimer()

		resp.VoteGranted = true
	}

	return resp
}
