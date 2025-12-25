package server

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const (
	heartbeatInterval = 800 * time.Millisecond
	RPCTimeout        = 400 * time.Millisecond
)

func (s *Server) resetElectionTimer() {
	// Random timeout: 150 ms - 300 ms
	// If all the servers timeout at the sae time, they all become candidates, causing failed elections.
	// Random timeout means one server usually becomes a candidate first.
	var timeout = time.Duration(800+rand.Intn(8001)) * time.Millisecond

	if s.electionTimer != nil {
		s.electionTimer.Stop()
	}

	s.electionTimer = time.NewTimer(timeout)
}

func (s *Server) startElection() {
	s.mx.Lock()

	// become a candidate
	s.state = Candidate

	fmt.Printf("[%d] Become Candidate\n", s.ID)

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

		// request votes for other peers
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

	fmt.Printf("[%d] Become Leader\n", s.ID)

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
	s.heartbeatTicker = time.NewTicker(heartbeatInterval)
	go s.sendHeartbeats()
}
