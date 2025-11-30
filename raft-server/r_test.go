package server

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

/*
======================================================================================
HOW prevLogIndex AND prevLogTerm WORK IN RAFT
======================================================================================

The prevLogIndex and prevLogTerm mechanism is Raft's way of ensuring LOG CONSISTENCY.

CONCEPT:
When a leader sends new log entries to a follower, it includes:
- prevLogIndex: the index of the log entry IMMEDIATELY BEFORE the new entries
- prevLogTerm: the term of that previous entry

The follower checks: "Do I have an entry at prevLogIndex with term prevLogTerm?"
- If YES → logs are consistent, append new entries
- If NO  → logs are inconsistent, reject the request

EXAMPLE SCENARIO:
Leader's log: [1:T1, 2:T1, 3:T2, 4:T2, 5:T3]
                                      ↑
                                   prevLogIndex=4, prevLogTerm=T2

Leader wants to send entry 5 (index=5, term=T3).
It sets prevLogIndex=4, prevLogTerm=T2 (the entry just before the new one).

Case 1 - Follower's log: [1:T1, 2:T1, 3:T2, 4:T2]
  ✓ Has entry at index 4 with term T2 → ACCEPT and append entry 5

Case 2 - Follower's log: [1:T1, 2:T1, 3:T2, 4:T3]
  ✗ Has entry at index 4, but term is T3 (not T2) → REJECT

Case 3 - Follower's log: [1:T1, 2:T1, 3:T2]
  ✗ Doesn't have entry at index 4 → REJECT

WHY THIS MATTERS:
This mechanism ensures that when a follower accepts new entries, its log matches
the leader's log up to that point. If logs don't match, the leader will decrement
nextIndex and retry with earlier entries until it finds the point where logs match.

SPECIAL CASE - prevLogIndex = 0:
When prevLogIndex=0, it means "no previous entry" (inserting at the beginning).
This check is skipped, allowing the first entries to be added to an empty log.

======================================================================================
*/

func TestAppendEntries_TableDriven(t *testing.T) {
	tests := []struct {
		name string
		// Initial state
		followerLog         []logEntry
		followerTerm        uint32
		followerCommitIndex uint32
		// Request
		request *AppendEntriesRequest
		// Expected results
		expectSuccess       bool
		expectedLogLength   int
		expectedCommitIndex uint32
		expectedTerm        uint32
		description         string
	}{
		{
			name:                "Heartbeat with empty log",
			followerLog:         []logEntry{},
			followerTerm:        1,
			followerCommitIndex: 0,
			request: &AppendEntriesRequest{
				Term:         1,
				LeaderID:     2,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      []logEntry{},
				LeaderCommit: 0,
			},
			expectSuccess:       true,
			expectedLogLength:   0,
			expectedCommitIndex: 0,
			expectedTerm:        1,
			description:         "Empty AppendEntries (heartbeat) should succeed and reset election timer",
		},
		{
			name:                "First entry to empty log",
			followerLog:         []logEntry{},
			followerTerm:        0,
			followerCommitIndex: 0,
			request: &AppendEntriesRequest{
				Term:         1,
				LeaderID:     2,
				PrevLogIndex: 0, // No previous entry
				PrevLogTerm:  0,
				Entries: []logEntry{
					{Index: 1, Term: 1, Command: []byte("cmd1")},
				},
				LeaderCommit: 0,
			},
			expectSuccess:       true,
			expectedLogLength:   1,
			expectedCommitIndex: 0,
			expectedTerm:        1,
			description:         "First entry with prevLogIndex=0 should append to empty log",
		},
		{
			name: "Append to existing log - matching prevLogIndex",
			followerLog: []logEntry{
				{Index: 1, Term: 1, Command: []byte("cmd1")},
				{Index: 2, Term: 1, Command: []byte("cmd2")},
			},
			followerTerm:        1,
			followerCommitIndex: 0,
			request: &AppendEntriesRequest{
				Term:         1,
				LeaderID:     2,
				PrevLogIndex: 2, // Has entry at index 2
				PrevLogTerm:  1, // With term 1
				Entries: []logEntry{
					{Index: 3, Term: 1, Command: []byte("cmd3")},
				},
				LeaderCommit: 0,
			},
			expectSuccess:       true,
			expectedLogLength:   3,
			expectedCommitIndex: 0,
			expectedTerm:        1,
			description:         "Should append when prevLogIndex matches existing entry",
		},
		{
			name: "Reject - missing prevLogIndex entry",
			followerLog: []logEntry{
				{Index: 1, Term: 1, Command: []byte("cmd1")},
			},
			followerTerm:        1,
			followerCommitIndex: 0,
			request: &AppendEntriesRequest{
				Term:         1,
				LeaderID:     2,
				PrevLogIndex: 4, // Follower doesn't have entry 4
				PrevLogTerm:  2,
				Entries: []logEntry{
					{Index: 5, Term: 2, Command: []byte("cmd5")},
				},
				LeaderCommit: 0,
			},
			expectSuccess:       false,
			expectedLogLength:   1, // Unchanged
			expectedCommitIndex: 0,
			expectedTerm:        1,
			description:         "Should reject when follower doesn't have entry at prevLogIndex",
		},
		{
			name: "Reject - prevLogIndex term mismatch",
			followerLog: []logEntry{
				{Index: 1, Term: 1, Command: []byte("cmd1")},
				{Index: 2, Term: 1, Command: []byte("cmd2")},
				{Index: 3, Term: 2, Command: []byte("wrong-cmd3")},
			},
			followerTerm:        2,
			followerCommitIndex: 0,
			request: &AppendEntriesRequest{
				Term:         3,
				LeaderID:     2,
				PrevLogIndex: 3, // Has entry 3, but...
				PrevLogTerm:  3, // Term doesn't match (has T2, not T3)
				Entries: []logEntry{
					{Index: 4, Term: 3, Command: []byte("cmd4")},
				},
				LeaderCommit: 0,
			},
			expectSuccess:       false,
			expectedLogLength:   3, // Unchanged
			expectedCommitIndex: 0,
			expectedTerm:        3, // Term updated
			description:         "Should reject when prevLogIndex exists but term doesn't match",
		},
		{
			name: "Multiple entries at once",
			followerLog: []logEntry{
				{Index: 1, Term: 1, Command: []byte("cmd1")},
				{Index: 2, Term: 1, Command: []byte("cmd2")},
			},
			followerTerm:        1,
			followerCommitIndex: 0,
			request: &AppendEntriesRequest{
				Term:         2,
				LeaderID:     2,
				PrevLogIndex: 2,
				PrevLogTerm:  1,
				Entries: []logEntry{
					{Index: 3, Term: 2, Command: []byte("cmd3")},
					{Index: 4, Term: 2, Command: []byte("cmd4")},
					{Index: 5, Term: 2, Command: []byte("cmd5")},
				},
				LeaderCommit: 0,
			},
			expectSuccess:       true,
			expectedLogLength:   5,
			expectedCommitIndex: 0,
			expectedTerm:        2,
			description:         "Should append multiple entries in one request (efficiency)",
		},
		{
			name:                "Reject - prevLogIndex too high for empty log",
			followerLog:         []logEntry{},
			followerTerm:        1,
			followerCommitIndex: 0,
			request: &AppendEntriesRequest{
				Term:         1,
				LeaderID:     2,
				PrevLogIndex: 5, // Way too high
				PrevLogTerm:  1,
				Entries: []logEntry{
					{Index: 6, Term: 1, Command: []byte("cmd6")},
				},
				LeaderCommit: 0,
			},
			expectSuccess:       false,
			expectedLogLength:   0,
			expectedCommitIndex: 0,
			expectedTerm:        1,
			description:         "Should reject when prevLogIndex is too high (follower missing entries)",
		},
		{
			name: "Update commit index from leader",
			followerLog: []logEntry{
				{Index: 1, Term: 1, Command: []byte("cmd1")},
				{Index: 2, Term: 1, Command: []byte("cmd2")},
				{Index: 3, Term: 1, Command: []byte("cmd3")},
			},
			followerTerm:        1,
			followerCommitIndex: 0,
			request: &AppendEntriesRequest{
				Term:         1,
				LeaderID:     2,
				PrevLogIndex: 3,
				PrevLogTerm:  1,
				Entries:      []logEntry{}, // Just heartbeat
				LeaderCommit: 2,            // Leader says entries 1-2 are committed
			},
			expectSuccess:       true,
			expectedLogLength:   3,
			expectedCommitIndex: 2, // Should update
			expectedTerm:        1,
			description:         "Should update commitIndex when leader indicates commits",
		},
		{
			name: "Reject - lower term request",
			followerLog: []logEntry{
				{Index: 1, Term: 2, Command: []byte("cmd1")},
			},
			followerTerm:        3,
			followerCommitIndex: 0,
			request: &AppendEntriesRequest{
				Term:         2, // Lower than follower's term
				LeaderID:     2,
				PrevLogIndex: 1,
				PrevLogTerm:  2,
				Entries: []logEntry{
					{Index: 2, Term: 2, Command: []byte("cmd2")},
				},
				LeaderCommit: 0,
			},
			expectSuccess:       false,
			expectedLogLength:   1,
			expectedCommitIndex: 0,
			expectedTerm:        3, // Unchanged
			description:         "Should reject requests from lower term",
		},
		{
			name: "Higher term - update term and accept",
			followerLog: []logEntry{
				{Index: 1, Term: 1, Command: []byte("cmd1")},
			},
			followerTerm:        1,
			followerCommitIndex: 0,
			request: &AppendEntriesRequest{
				Term:         3, // Higher than follower's term
				LeaderID:     2,
				PrevLogIndex: 1,
				PrevLogTerm:  1,
				Entries: []logEntry{
					{Index: 2, Term: 3, Command: []byte("cmd2")},
				},
				LeaderCommit: 0,
			},
			expectSuccess:       true,
			expectedLogLength:   2,
			expectedCommitIndex: 0,
			expectedTerm:        3, // Should update
			description:         "Should update term and accept when request has higher term",
		},
		{
			name: "Commit index with new entries",
			followerLog: []logEntry{
				{Index: 1, Term: 1, Command: []byte("cmd1")},
			},
			followerTerm:        1,
			followerCommitIndex: 0,
			request: &AppendEntriesRequest{
				Term:         1,
				LeaderID:     2,
				PrevLogIndex: 1,
				PrevLogTerm:  1,
				Entries: []logEntry{
					{Index: 2, Term: 1, Command: []byte("cmd2")},
					{Index: 3, Term: 1, Command: []byte("cmd3")},
				},
				LeaderCommit: 3, // Leader has committed up to 3
			},
			expectSuccess:       true,
			expectedLogLength:   3,
			expectedCommitIndex: 3, // Should commit new entries
			expectedTerm:        1,
			description:         "Should update commit index to last new entry when leaderCommit > lastNewIndex",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test server
			server := setupTestServer(t, 1, []uint32{1, 2, 3})
			defer server.Shutdown()

			// Set up initial state
			server.persistentState.log = tt.followerLog
			server.persistentState.currentTerm = tt.followerTerm
			server.volatileState.commitedIndex = tt.followerCommitIndex

			// Log initial state
			t.Logf("Initial state:")
			t.Logf("  Follower log: %v (length=%d)", formatLog(tt.followerLog), len(tt.followerLog))
			t.Logf("  Follower term: %d", tt.followerTerm)
			t.Logf("  Commit index: %d", tt.followerCommitIndex)
			t.Logf("")
			t.Logf("Request:")
			t.Logf("  Term: %d, LeaderID: %d", tt.request.Term, tt.request.LeaderID)
			t.Logf("  PrevLogIndex: %d, PrevLogTerm: %d", tt.request.PrevLogIndex, tt.request.PrevLogTerm)
			t.Logf("  Entries: %v (count=%d)", formatLog(tt.request.Entries), len(tt.request.Entries))
			t.Logf("  LeaderCommit: %d", tt.request.LeaderCommit)
			t.Logf("")
			t.Logf("Expected: %s", tt.description)

			// Handle the request
			resp := server.HandleAppendEntries(tt.request)

			// Verify results
			require.Equal(t, tt.expectSuccess, resp.Success,
				"Success flag mismatch")
			require.Equal(t, tt.expectedLogLength, len(server.persistentState.log),
				"Log length mismatch")
			require.Equal(t, tt.expectedCommitIndex, server.volatileState.commitedIndex,
				"Commit index mismatch")
			require.Equal(t, tt.expectedTerm, server.persistentState.currentTerm,
				"Term mismatch")

			// Log results
			t.Logf("")
			t.Logf("Result:")
			t.Logf("  Success: %v", resp.Success)
			t.Logf("  Final log: %v (length=%d)", formatLog(server.persistentState.log), len(server.persistentState.log))
			t.Logf("  Final term: %d", server.persistentState.currentTerm)
			t.Logf("  Final commit index: %d", server.volatileState.commitedIndex)

			if tt.expectSuccess {
				t.Logf("✓ Test passed: %s", tt.description)
			} else {
				t.Logf("✓ Test passed (correctly rejected): %s", tt.description)
			}
		})
	}
}

// TestAppendEntries_RetryMechanism demonstrates the backtracking process
func TestAppendEntries_RetryMechanism(t *testing.T) {
	tests := []struct {
		attempt         int
		prevLogIndex    uint32
		prevLogTerm     uint32
		entryIndex      uint32
		expectSuccess   bool
		description     string
		leaderNextIndex uint32 // What nextIndex becomes after this attempt
	}{
		{
			attempt:         1,
			prevLogIndex:    5,
			prevLogTerm:     1,
			entryIndex:      6,
			expectSuccess:   false,
			description:     "Leader thinks follower has 5 entries, tries to send entry 6",
			leaderNextIndex: 5,
		},
		{
			attempt:         2,
			prevLogIndex:    4,
			prevLogTerm:     1,
			entryIndex:      5,
			expectSuccess:   false,
			description:     "Rejected - leader decrements nextIndex and tries entry 5",
			leaderNextIndex: 4,
		},
		{
			attempt:         3,
			prevLogIndex:    3,
			prevLogTerm:     1,
			entryIndex:      4,
			expectSuccess:   false,
			description:     "Rejected - leader decrements nextIndex and tries entry 4",
			leaderNextIndex: 3,
		},
		{
			attempt:         4,
			prevLogIndex:    2,
			prevLogTerm:     1,
			entryIndex:      3,
			expectSuccess:   true,
			description:     "Success! Found matching point at index 2, can append entry 3",
			leaderNextIndex: 4,
		},
	}

	// Follower has: [1:T1, 2:T1]
	server := setupTestServer(t, 1, []uint32{1, 2, 3})
	defer server.Shutdown()

	server.persistentState.log = []logEntry{
		{Index: 1, Term: 1, Command: []byte("cmd1")},
		{Index: 2, Term: 1, Command: []byte("cmd2")},
	}
	server.persistentState.currentTerm = 1

	t.Log("=================================================================")
	t.Log("RETRY MECHANISM DEMONSTRATION")
	t.Log("=================================================================")
	t.Log("")
	t.Log("Scenario: Leader thinks follower has 5 entries, but follower only has 2")
	t.Log("Follower state: [1:T1, 2:T1]")
	t.Log("Leader's nextIndex for this follower: 6 (too high!)")
	t.Log("")
	t.Log("Leader will backtrack by decrementing nextIndex until finding match point")
	t.Log("=================================================================")
	t.Log("")

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			t.Logf("--- Attempt %d ---", tt.attempt)
			t.Logf("Leader tries: prevLogIndex=%d, prevLogTerm=%d, new entry index=%d",
				tt.prevLogIndex, tt.prevLogTerm, tt.entryIndex)

			req := &AppendEntriesRequest{
				Term:         1,
				LeaderID:     2,
				PrevLogIndex: tt.prevLogIndex,
				PrevLogTerm:  tt.prevLogTerm,
				Entries: []logEntry{
					{Index: tt.entryIndex, Term: 1, Command: []byte("cmd")},
				},
				LeaderCommit: 0,
			}

			resp := server.HandleAppendEntries(req)

			require.Equal(t, tt.expectSuccess, resp.Success, tt.description)

			if resp.Success {
				t.Logf("✓ SUCCESS - %s", tt.description)
				t.Logf("  Follower log: %v", formatLog(server.persistentState.log))
				t.Logf("  Leader updates nextIndex: %d → %d", tt.prevLogIndex+1, tt.leaderNextIndex)
			} else {
				t.Logf("✗ REJECTED - %s", tt.description)
				t.Logf("  Leader decrements nextIndex: %d → %d", tt.prevLogIndex+1, tt.leaderNextIndex)
			}
			t.Log("")
		})
	}

	t.Log("=================================================================")
	t.Log("After finding match point, leader can continue sending entries 4, 5, 6...")
	t.Log("=================================================================")
}

// formatLog creates a readable string representation of log entries
func formatLog(log []logEntry) string {
	if len(log) == 0 {
		return "[]"
	}

	result := "["
	for i, entry := range log {
		if i > 0 {
			result += ", "
		}
		result += fmt.Sprintf("%d:T%d", entry.Index, entry.Term)
	}
	result += "]"
	return result
}
