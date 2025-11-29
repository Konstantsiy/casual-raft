package server

import (
	"fmt"
	"github.com/Konstantsiy/casual-raft/state-machine"
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
	client RaftClient

	// signal to spot all goroutines
	shutdownCh chan struct{}
}

func NewServer(id uint32, peers []uint32, dataDir string, client RaftClient) (*Server, error) {
	dirPath := fmt.Sprintf("%s/server-%d.dat", dataDir, id)
	fd, err := os.OpenFile(dirPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	server := &Server{
		ID:    id,
		peers: peers,
		fd:    fd,
		state: Follower,
		leaderState: leaderState{
			nextIndex:  make(map[uint32]uint32),
			matchIndex: make(map[uint32]uint32),
		},
		volatileState: volatileState{
			commitedIndex: 0,
			lastApplied:   0,
		},
		sm:         state_machine.New(),
		client:     client,
		shutdownCh: make(chan struct{}),
	}

	if err = server.restore(); err != nil {
		server.persistentState.log = make([]logEntry, 0)
		server.persistentState.currentTerm = 0
		server.persistentState.votedFor = 0
	}

	return server, nil
}

func (s *Server) Start() {
	fmt.Printf("[%d] Started\n", s.ID)
	// start election timer, needed for follower and candidate states,
	// because leaders don't hold elections, they stop the timer
	s.resetElectionTimer()
	fmt.Printf("[%d] Timer reset\n", s.ID)

	//time.Sleep(10 * time.Millisecond)

	go func() {
		// main cycle for each server, it waits for events and handle them
		for {
			select {
			case <-s.shutdownCh:
				fmt.Printf("[%d] Shutdown signal received\n", s.ID)
				return

			case <-s.electionTimer.C:
				// election timer fired - no heartbeat from leader
				fmt.Printf("[%d] Election timer fired\n", s.ID)
				s.startElection()
			}
		}
	}()
}

func (s *Server) Shutdown() {
	close(s.shutdownCh)

	if s.electionTimer != nil {
		s.electionTimer.Stop()
	}

	if s.heartbeatTicker != nil {
		s.heartbeatTicker.Stop()
	}

	_ = s.fd.Close()
}

func (s *Server) State() (uint32, bool) {
	s.mx.Lock()
	defer s.mx.Unlock()

	return s.persistentState.currentTerm, s.state == Leader
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
	var entries []logEntry
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
