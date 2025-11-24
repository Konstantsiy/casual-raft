package server

import (
	"github.com/Konstantsiy/casual-raft"
	"github.com/Konstantsiy/casual-raft/state-machine"
	"math/rand"
	"os"
	"sync"
	"time"
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
	state State

	electionTimer   *time.Timer  // timer that triggers election if no heartbeat received
	heartbeatTicker *time.Ticker // ticker that sends periodic heartbeats

	sm     state_machine.StateMachine
	client *casual_raft.RaftClient

	// signal to spot all goroutines
	shutdownCh chan struct{}
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
			if s.state != Leader {
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

	if s.state != Leader {
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
	var entries []casual_raft.LogEntry
	for _, entry := range s.persistentState.log {
		if entry.Index >= nextIndex {
			entries = append(entries, entry)
		}
	}

	var req = &casual_raft.AppendEntriesRequest{
		Term:         s.persistentState.currentTerm,
		LeaderID:     s.ID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: s.volatileState.commitedIndex, // tell follower what's commited
	}

	s.mx.RUnlock()

	var resp, err = s.client.SendAppendEntries(peerID, req)
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
		s.state = Follower
		s.persistentState.votedFor = 0

		_ = s.persist()
		s.resetElectionTimer()

		if s.heartbeatTicker != nil {
			s.heartbeatTicker.Stop()
		}

		return
	}

	// if we're still leader, process the result
	if s.state != Leader {
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
	if s.state != Leader {
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
	s.state = Candidate

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
			var req = &casual_raft.RequestVoteRequest{
				Term:         currentTerm,
				CandidateID:  s.ID,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			// might fail in peer is down/slow
			var resp, err = s.client.SendRequestVote(peer, req)
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
				s.state = Follower
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
	if s.state != Candidate {
		return
	}

	s.state = Leader

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
