package server

import (
	"encoding/binary"
	"fmt"
)

type State int

const (
	// Follower - normal state, receives commands from leader
	// If no heartbeats received, becomes candidate
	Follower = iota

	// Candidate - trying to become leader, requests votes from other servers
	Candidate

	// Leader - receives client requests and replicates to followers
	// Only 1 leader at a time in the cluster
	Leader
)

// persistentState is the state that MUST BE persisted on all servers and survive crashes
type persistentState struct {
	// currentTerm is the latest term server has seen
	// (initialized to 0 on first boot, increases monotonically)
	currentTerm uint32

	// votedFor marks which candidate did we vote for in the current term
	// 0 == haven't voted yet
	votedFor uint32

	// log is a sequence of commands for state machine
	log []logEntry
}

// volatileState represents data that can be rebuilt after a crash, kept im memory
type volatileState struct {
	// commitedIndex is the highest log entry known to be commited
	commitedIndex uint32

	// lastApplied is the highest log entry applied to state machine
	lastApplied uint32
}

// leaderState is the data that server tracks about what each follower has replicated
type leaderState struct {
	// nextIndex: for each server, index of the next log entry to send
	// Initialized to (last log index + 1)
	// If append fails, decrement and retry
	nextIndex map[uint32]uint32

	// matchIndex: for each server: highest log entry known to be replicated,
	// Used to determine when entries are commited (majority rule)
	matchIndex map[uint32]uint32
}

func (s *Server) persistLocked() error {
	s.mx.Lock()
	defer s.mx.Unlock()

	return s.persist()
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

	s.persistentState.log = make([]logEntry, 0, logLength)

	for i := uint32(0); i < logLength; i++ {
		var entryHeader = make([]byte, 12)
		if _, err = s.fd.Read(entryHeader); err != nil {
			return fmt.Errorf("cannot read [%d] log entry header: %v", i, err)
		}

		var entry logEntry
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
