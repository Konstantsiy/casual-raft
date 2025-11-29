package server

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func setupTestServer(t *testing.T, serverID uint32, peers []uint32) *Server {
	var testDir = t.TempDir()
	var testClient = NewRaftClient([]string{})

	server, err := NewServer(serverID, peers, testDir, testClient)
	require.NoError(t, err)

	return server
}

func TestServer_PersisAndRestore(t *testing.T) {
	testDir := t.TempDir()
	testClient := NewRaftClient([]string{})

	server1, err := NewServer(1, []uint32{1, 2, 3}, testDir, testClient)
	require.NoError(t, err)

	// write some state to server1
	server1.persistentState.currentTerm = 5
	server1.persistentState.votedFor = 2
	server1.persistentState.log = []logEntry{
		{Index: 1, Term: 1, Command: []byte("cmd1")},
		{Index: 2, Term: 2, Command: []byte("cmd2")},
	}

	// save server1 state
	err = server1.persist()
	require.NoError(t, err)

	server1.Shutdown()

	// restore first server
	server2, err := NewServer(1, []uint32{1, 2, 3}, testDir, testClient)
	require.NoError(t, err)
	defer server2.Shutdown()

	// state should be restored
	require.Equal(t, uint32(5), server2.persistentState.currentTerm)
	require.Equal(t, uint32(2), server2.persistentState.votedFor)
	require.Len(t, server1.persistentState.log, 2)
}

func TestServer_RequestVote(t *testing.T) {
	// term: 0
	server := setupTestServer(t, 1, []uint32{1, 2, 3})
	defer server.Shutdown()

	req1 := &RequestVoteRequest{
		Term:         1,
		CandidateID:  2,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	// grant vote to a candidate with up-to-date log
	resp := server.HandleRequestVote(req1)
	require.Equal(t, uint32(1), server.persistentState.currentTerm)
	require.True(t, resp.VoteGranted)
	require.Equal(t, uint32(2), server.persistentState.votedFor)

	req2 := &RequestVoteRequest{
		Term:         1,
		CandidateID:  3,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	// reject vote for a different candidate in the same term
	resp2 := server.HandleRequestVote(req2)
	require.False(t, resp2.VoteGranted)

	req3 := &RequestVoteRequest{
		Term:         2,
		CandidateID:  3,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	// grant vote for a different candidate in new term
	resp3 := server.HandleRequestVote(req3)
	require.True(t, resp3.VoteGranted)
}

func TestServer_AppendEntries(t *testing.T) {
	server := setupTestServer(t, 1, []uint32{1, 2, 3})
	defer server.Shutdown()

	// just a heartbeat with an empty entries
	req := &AppendEntriesRequest{
		Term:         1,
		LeaderID:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []logEntry{},
		LeaderCommit: 0,
	}

	resp := server.HandleAppendEntries(req)
	require.True(t, resp.Success)
	require.Equal(t, uint32(1), server.persistentState.currentTerm)

	// append new entry
	req = &AppendEntriesRequest{
		Term:         1,
		LeaderID:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []logEntry{
			{Index: 1, Term: 1, Command: []byte("cmd1")},
		},
		LeaderCommit: 0,
	}

	resp = server.HandleAppendEntries(req)
	require.True(t, resp.Success)
	require.Len(t, server.persistentState.log, 1)

	// reject request, because prevLogIndex doesn't match
	req = &AppendEntriesRequest{
		Term:         1,
		LeaderID:     2,
		PrevLogIndex: 5,
		PrevLogTerm:  1,
		Entries: []logEntry{
			{Index: 6, Term: 1, Command: []byte("cmd1")},
		},
		LeaderCommit: 0,
	}

	resp = server.HandleAppendEntries(req)
	require.False(t, resp.Success)
}
