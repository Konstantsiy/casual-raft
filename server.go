package casual_raft

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"
)

type ServerState int

const (
	// ServerStateFollower - normal state, receives commands from leader
	// If no heartbeats received, becomes candidate
	ServerStateFollower = iota

	// ServerStateCandidate - trying to become leader, requests votes from other servers
	ServerStateCandidate

	// ServerStateLeader - receives client requests and replicates to followers
	// Only 1 leader at a time in the cluster
	ServerStateLeader
)

type Server struct {
	ID    uint32
	peers []uint32 // all server ID's in cluster

	mx sync.RWMutex

	persistentState persistentState // state written to disk
	fd              *os.File        // fd is a file descriptor to store persistence state
	volatileState   volatileState   //  for each server
	leaderState     leaderState     // only used when state == Leader

	// current state
	state ServerState

	electionTimer   *time.Timer  // timer that triggers election if no heartbeat received
	heartbeatTicker *time.Ticker // ticker that sends periodic heartbeats

	sm     StateMachine
	client *raftClient

	// signal to spot all goroutines
	shutdownCh chan struct{}
}

// persist writes the persistent state to disk
/*
	The persistent state format is:
	[0..3]   - currentTerm (4 bytes)
	[4..7]   - votedFor    (4 bytes)
	[8..11]  - logLength   (4 bytes)
	[12..]   - logs, sequence of entries
	each line = one entry  with format:
	[0..3]  - term (uint32)
	[4..7]  - index (uint32)
	[8..11] - command length (uint32)
	[12..]  - command bytes
*/
func (s *Server) persist() error {
	s.mx.Lock()
	defer s.mx.Unlock()

	var err error
	if err = s.fd.Truncate(0); err != nil {
		return err
	}

	if _, err = s.fd.Seek(0, 0); err != nil {
		return err
	}

	var header = make([]byte, 12)
	binary.BigEndian.PutUint32(header[0:4], s.persistentState.currentTerm)
	binary.BigEndian.PutUint32(header[4:8], s.persistentState.votedFor)
	binary.BigEndian.PutUint32(header[8:12], uint32(len(s.persistentState.log)))

	if _, err = s.fd.Write(header); err != nil {
		return fmt.Errorf("cannot write persistent state header: %v", err)
	}

	for i, entry := range s.persistentState.log {
		var entryHeader = make([]byte, 12)
		binary.BigEndian.PutUint32(entryHeader[0:4], entry.Term)
		binary.BigEndian.PutUint32(entryHeader[4:8], entry.Index)
		binary.BigEndian.PutUint32(entryHeader[8:12], uint32(len(entry.Command)))

		if _, err = s.fd.Write(entryHeader); err != nil {
			return fmt.Errorf("cannot write [%d] log entry header: %v", i, err)
		}

		if _, err = s.fd.Write(entry.Command); err != nil {
			return fmt.Errorf("cannot write [%d] log entry command: %v", i, err)
		}
	}

	if err = s.fd.Sync(); err != nil {
		return fmt.Errorf("cannot sync persistent state to disk: %v", err)
	}

	return nil
}

func (s *Server) restore() error {
	s.mx.Lock()
	defer s.mx.Unlock()

	var err error
	if _, err = s.fd.Seek(0, 0); err != nil {
		return err
	}

	var header = make([]byte, 12)
	if _, err = s.fd.Read(header); err != nil {
		return fmt.Errorf("cannot read persistent state header: %v", err)
	}

	s.persistentState.currentTerm = binary.BigEndian.Uint32(header[0:4])
	s.persistentState.votedFor = binary.BigEndian.Uint32(header[4:8])
	var logLength = binary.BigEndian.Uint32(header[8:12])

	s.persistentState.log = make([]LogEntry, 0, logLength)

	for i := uint32(0); i < logLength; i++ {
		var entryHeader = make([]byte, 12)
		if _, err = s.fd.Read(entryHeader); err != nil {
			return fmt.Errorf("cannot read [%d] log entry header: %v", i, err)
		}

		var entry LogEntry
		entry.Term = binary.BigEndian.Uint32(entryHeader[0:4])
		entry.Index = binary.BigEndian.Uint32(entryHeader[4:8])
		var cmdLen = binary.BigEndian.Uint32(entryHeader[8:12])

		entry.Command = make([]byte, cmdLen)
		if _, err = s.fd.Read(entry.Command); err != nil {
			return fmt.Errorf("cannot read [%d] log entry command: %v", i, err)
		}

		s.persistentState.log = append(s.persistentState.log, entry)
	}

	return nil
}

func (s *Server) resetElectionTimer() {
	// Random timeout: 150 ms - 300 ms
	// If all the servers timeout at the sae time, they all become candidates, causing failed elections.
	// Random timeout means one server usually becomes a candidate first.
	var timeout = time.Duration(150*rand.Intn(150)) * time.Millisecond

	if s.electionTimer != nil {
		s.electionTimer.Stop()
	}

	s.electionTimer = time.NewTimer(timeout)
}

// run is the main cycle for each server, it waits for events and handle them
func (s *Server) run() {
	for {
		select {
		case <-s.shutdownCh:
			return

		// election timer fired - no heartbeat from leader
		case <-s.electionTimer.C:
			s.startElection()
		}
	}
}

func (s *Server) sendHeartbeats() {
	for {
		select {
		case <-s.shutdownCh:
			return

		case <-s.heartbeatTicker.C:
			// check if still leader
			s.mx.RLock()
			if s.state != ServerStateLeader {
				s.mx.RUnlock()
				return
			}
			s.mx.RUnlock()

			// send AppendEntries to each peer
			for _, peerID := range s.peers {
				if peerID == s.ID {
					continue
				}

				go s.replicateLog(peerID)
			}
		}
	}
}

func (s *Server) replicateLog(peerID uint32) {
	s.mx.RLock()

	if s.state != ServerStateLeader {
		s.mx.RUnlock()
		return
	}

	// determine what to send to peer,
	// nextIndex[peer] - where to start from
	var nextIndex = s.leaderState.nextIndex[peerID]

	// build the "consistency check" params
	// prevLogIndex - log entry before new one
	// prevLogTerm - term of that log entry
	// Follower checks if it has matching entry and previous log index,
	// If not, logs are inconsistent and follower rejects
	var prevLogIndex = nextIndex - 1
	var prevLogTerm = uint32(0)

	if prevLogIndex > 0 {
		for _, entry := range s.persistentState.log {
			if entry.Index == prevLogIndex {
				prevLogTerm = entry.Term
				break
			}
		}
	}

	// collect every entry from nextIndex onwards to send
	var entries []LogEntry
	for _, entry := range s.persistentState.log {
		if entry.Index >= nextIndex {
			entries = append(entries, entry)
		}
	}

	var req = &AppendEntriesRequest{
		Term:         s.persistentState.currentTerm,
		LeaderID:     s.ID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: s.volatileState.commitedIndex, // tell follower what's commited
	}

	s.mx.RUnlock()

	var resp, err = s.client.sendAppendEntries(peerID, req)
	if err != nil {
		// todo: log
		return
	}

	s.mx.Lock()
	s.mx.Unlock()

	// check if peer has higher term
	if resp.Term != s.persistentState.currentTerm {
		// we're behind, need to step down to the follower state
		s.persistentState.currentTerm = resp.Term
		s.state = ServerStateFollower
		s.persistentState.votedFor = 0

		_ = s.persist()
		s.resetElectionTimer()

		if s.heartbeatTicker != nil {
			s.heartbeatTicker.Stop()
		}

		return
	}

	// if we're still leader, process the result
	if s.state != ServerStateLeader {
		return
	}

	// if log inconsistent or replication was fail, decrement next index and retry (on next heartbeat)
	if !resp.Success {
		if s.leaderState.nextIndex[peerID] > 1 {
			s.leaderState.nextIndex[peerID]--
		}
	}

	// if peer successfully replicated the entries, update our tracking
	if len(entries) > 0 {
		s.leaderState.matchIndex[peerID] = entries[len(entries)-1].Index
		s.leaderState.nextIndex[peerID] = entries[len(entries)-1].Index + 1
	}

	s.updateCommitIndex()
}

func (s *Server) updateCommitIndex() {
	// only reader can commit index
	if s.state != ServerStateLeader {
		return
	}

	for n := s.volatileState.commitedIndex + 1; ; n++ {
		// count how many servers have this index
		count := 1 // count self
		hasEntry := false

		// find the entry at index n
		for _, entry := range s.persistentState.log {
			if entry.Index == n {
				hasEntry = true

				// only commit if from current term
				if entry.Term == s.persistentState.currentTerm {
					// count replicas
					for _, peerID := range s.peers {
						if peerID != s.ID && s.leaderState.matchIndex[peerID] >= n {
							count++
						}
					}
				}
				break
			}
		}

		if !hasEntry {
			break // no more entries to check
		}

		// do we have a majority?
		if count >= len(s.peers)/2+1 {
			// yes, This entry is committed
			s.volatileState.commitedIndex = n
		} else {
			break // haven't reached majority yet
		}
	}

	// Apply committed entries to state machine
	s.applyCommitedEntries()
}

// applyCommitedEntries applies commited log entries and commited index
func (s *Server) applyCommitedEntries() {
	for s.volatileState.lastApplied < s.volatileState.commitedIndex {
		s.volatileState.lastApplied++

		// find the entry to apply
		for _, entry := range s.persistentState.log {
			if entry.Index == s.volatileState.lastApplied {
				_, _ = s.sm.Apply(entry.Command)
				break
			}
		}
	}
}

func (s *Server) startElection() {
	s.mx.Lock()

	// become a candidate
	s.state = ServerStateCandidate

	// increment term (new election round)
	s.persistentState.currentTerm++
	var currentTerm = s.persistentState.currentTerm

	// voted for yourself
	s.persistentState.votedFor = s.ID

	_ = s.persist()

	var lastLogIndex = uint32(0)
	var lastLogTerm = uint32(0)

	if len(s.persistentState.log) > 0 {
		var lastEntry = s.persistentState.log[len(s.persistentState.log)-1]
		lastLogIndex = lastEntry.Index
		lastLogTerm = lastEntry.Term
	}

	s.mx.Unlock()

	// reset time for the next election if this fails
	s.resetElectionTimer()

	// collect votes from all peers, start with 1 vote (yourself)
	var votes = 1
	var voteMx sync.Mutex

	for _, peerID := range s.peers {
		if peerID == s.ID {
			continue
		}

		go func(peer uint32) {
			var req = &RequestVoteRequest{
				Term:         currentTerm,
				CandidateID:  s.ID,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			// might fail in peer is down/slow
			var resp, err = s.client.sendRequestVote(peer, req)
			if err != nil {
				// just ignore unreachable peer
				// todo: add logging
				return
			}

			// check if peer has higher term
			s.mx.Lock()
			if resp.Term > s.persistentState.currentTerm {
				// we're behind, need to step down
				s.persistentState.currentTerm = resp.Term
				s.state = ServerStateFollower
				s.persistentState.votedFor = 0

				_ = s.persist()

				s.resetElectionTimer()
				s.mx.Unlock()

				// we don't need to check the other peers anymore
				return
			}
			s.mx.Unlock()

			// count the vote if granted
			if resp.VoteGranted {
				voteMx.Lock()
				votes++

				// check if we ween taken the majority of votes
				var majority = len(s.peers)/2 + 1
				if votes >= majority {
					s.becomeLeader()
				}
				voteMx.Unlock()
			}

		}(peerID)
	}
}

func (s *Server) becomeLeader() {
	s.mx.Lock()
	defer s.mx.Unlock()

	// only become leader if still a candidate
	if s.state != ServerStateCandidate {
		return
	}

	s.state = ServerStateLeader

	// init leader state
	// for each peer: track what they have replicated
	var lastLogIndex = uint32(0)
	if len(s.persistentState.log) > 0 {
		lastLogIndex = s.persistentState.log[len(s.persistentState.log)-1].Index
	}

	for _, peerID := range s.peers {
		if peerID != s.ID {
			s.leaderState.nextIndex[peerID] = lastLogIndex + 1
			s.leaderState.matchIndex[peerID] = 0
		}
	}

	// stop election timer, because leaders don't hold elections
	if s.electionTimer != nil {
		s.electionTimer.Stop()
	}

	// heartbeats are just empty AppendEntries RPC's,
	// they prevent followers from starting elections
	s.heartbeatTicker = time.NewTicker(50 * time.Millisecond)
	go s.sendHeartbeats()
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
		s.state = ServerStateFollower
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
