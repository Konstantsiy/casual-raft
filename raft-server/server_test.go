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

func TestServer_LogReplication(t *testing.T) {
	tt := []struct {
		name                string
		followerLogs        []logEntry
		followerTerm        uint32
		followerCommitIndex uint32
		req                 *AppendEntriesRequest
		expectedSuccess     bool
		expectedLogLength   int
		expectedCommitIndex uint32
		expectedTerm        uint32
	}{
		{
			// empty appendEntries (just a heartbeat) should succeed and reset election timer
			name:                "reject: heartbeat with empty log",
			followerLogs:        []logEntry{},
			followerTerm:        1,
			followerCommitIndex: 0,
			req: &AppendEntriesRequest{
				Term:         1,
				LeaderID:     2,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      []logEntry{},
				LeaderCommit: 0,
			},
			expectedSuccess:     true,
			expectedLogLength:   0,
			expectedCommitIndex: 0,
			expectedTerm:        1,
		},
		{
			// the first entry with prevLogIndex=0 should append to empty log
			name:                "append: first entry to empty log",
			followerLogs:        []logEntry{},
			followerTerm:        0,
			followerCommitIndex: 0,
			req: &AppendEntriesRequest{
				Term:         1,
				LeaderID:     2,
				PrevLogIndex: 0, // no previous entry
				PrevLogTerm:  0,
				Entries: []logEntry{
					{Index: 1, Term: 1, Command: []byte("cmd1")},
				},
				LeaderCommit: 0,
			},
			expectedSuccess:     true,
			expectedLogLength:   1,
			expectedCommitIndex: 0,
			expectedTerm:        1,
		},
		{
			// should append when prevLogIndex matches existing entry
			name: "append: matching prevLogIndex",
			followerLogs: []logEntry{
				{Term: 1, Index: 1, Command: []byte("cmd1")},
				{Term: 1, Index: 2, Command: []byte("cmd2")},
			},
			followerTerm:        1,
			followerCommitIndex: 0,
			req: &AppendEntriesRequest{
				Term:         1,
				LeaderID:     2,
				PrevLogTerm:  1, // the same previous log term and index
				PrevLogIndex: 2,
				Entries: []logEntry{
					{Term: 1, Index: 3, Command: []byte("cmd3")},
				},
				LeaderCommit: 0,
			},
			expectedSuccess:     true,
			expectedLogLength:   3,
			expectedCommitIndex: 0,
			expectedTerm:        1,
		},
		{
			// should reject when follower doesn't have entry at prevLogIndex
			name: "reject: missing prevLogIndex entry",
			followerLogs: []logEntry{
				{Term: 1, Index: 1, Command: []byte("cmd1")},
			},
			followerTerm:        1,
			followerCommitIndex: 0,
			req: &AppendEntriesRequest{
				Term:         1,
				LeaderID:     2,
				PrevLogIndex: 4, // follower doesn't have entry 4
				PrevLogTerm:  2,
				Entries: []logEntry{
					{Term: 2, Index: 5, Command: []byte("cmd5")},
				},
				LeaderCommit: 0,
			},
			expectedSuccess:     false,
			expectedLogLength:   1,
			expectedCommitIndex: 0,
			expectedTerm:        1,
		},
		{
			// should reject when prevLogIndex exists but the term doesn't match
			name: "reject: prevLogIndex term mismatch",
			followerLogs: []logEntry{
				{Term: 1, Index: 1, Command: []byte("cmd1")},
				{Term: 1, Index: 2, Command: []byte("cmd2")},
				{Term: 2, Index: 3, Command: []byte("wrong-cmd3")},
			},
			followerTerm:        2,
			followerCommitIndex: 0,
			req: &AppendEntriesRequest{
				Term:         3,
				LeaderID:     2,
				PrevLogTerm:  3, // term doesn't match
				PrevLogIndex: 3,
				Entries: []logEntry{
					{Term: 3, Index: 4, Command: []byte("cmd4")},
				},
				LeaderCommit: 0,
			},
			expectedSuccess:     false,
			expectedLogLength:   3,
			expectedCommitIndex: 0,
			expectedTerm:        3, // the term will be updated
		},
		{
			name: "append: multiple entries at once",
			followerLogs: []logEntry{
				{Term: 1, Index: 1, Command: []byte("cmd1")},
				{Term: 1, Index: 2, Command: []byte("cmd2")},
			},
			followerTerm:        1,
			followerCommitIndex: 0,
			req: &AppendEntriesRequest{
				Term:         2,
				LeaderID:     2,
				PrevLogTerm:  1,
				PrevLogIndex: 2,
				Entries: []logEntry{
					{Term: 1, Index: 3, Command: []byte("cmd3")},
					{Term: 2, Index: 4, Command: []byte("cmd4")},
					{Term: 2, Index: 5, Command: []byte("cmd5")},
				},
				LeaderCommit: 0,
			},
			expectedSuccess:     true,
			expectedLogLength:   5,
			expectedCommitIndex: 0,
			expectedTerm:        2,
		},
		{
			// should reject when prevLogIndex is too high (follower missing entries)
			name:                "reject: prevLogIndex too high for empty log",
			followerLogs:        []logEntry{},
			followerTerm:        1,
			followerCommitIndex: 0,
			req: &AppendEntriesRequest{
				Term:         1,
				LeaderID:     2,
				PrevLogIndex: 5, // too high
				PrevLogTerm:  1,
				Entries: []logEntry{
					{Index: 6, Term: 1, Command: []byte("cmd6")},
				},
				LeaderCommit: 0,
			},
			expectedSuccess:     false,
			expectedLogLength:   0,
			expectedCommitIndex: 0,
			expectedTerm:        1,
		},
		{
			name: "append: update commit index from leader",
			followerLogs: []logEntry{
				{Index: 1, Term: 1, Command: []byte("cmd1")},
				{Index: 2, Term: 1, Command: []byte("cmd2")},
				{Index: 3, Term: 1, Command: []byte("cmd3")},
			},
			followerTerm:        1,
			followerCommitIndex: 0,
			req: &AppendEntriesRequest{
				Term:         1,
				LeaderID:     2,
				PrevLogIndex: 3,
				PrevLogTerm:  1,
				Entries:      []logEntry{}, // just a heartbeat
				LeaderCommit: 2,            // leader says entries 1-2 are committed
			},
			expectedSuccess:     true,
			expectedLogLength:   3,
			expectedCommitIndex: 2, // should be updated
			expectedTerm:        1,
		},
	}

	for _, _tc := range tt {
		tc := _tc
		t.Run(tc.name, func(t *testing.T) {
			// init a test server as a Follower
			server := setupTestServer(t, 1, []uint32{1, 2, 3})
			defer server.Shutdown()

			// setup initial state
			server.persistentState.log = tc.followerLogs
			server.persistentState.currentTerm = tc.followerTerm
			server.volatileState.commitedIndex = tc.followerCommitIndex

			resp := server.HandleAppendEntries(tc.req)

			require.Equal(t, tc.expectedSuccess, resp.Success,
				"Success flag mismatch")
			require.Equal(t, tc.expectedLogLength, len(server.persistentState.log),
				"Log length mismatch")
			require.Equal(t, tc.expectedCommitIndex, server.volatileState.commitedIndex,
				"Commit index mismatch")
			require.Equal(t, tc.expectedTerm, server.persistentState.currentTerm,
				"Term mismatch")
		})
	}
}
