package server

import (
	casualraft "github.com/Konstantsiy/casual-raft"
)

func (s *Server) HandleAppendEntries(req *casualraft.AppendEntriesRequest) *casualraft.AppendEntriesResponse {
	s.mx.Lock()
	defer s.mx.Unlock()

	var resp = &casualraft.AppendEntriesResponse{
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
