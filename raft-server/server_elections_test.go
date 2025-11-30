package server

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type mockCluster struct {
	t *testing.T

	mockClient RaftClient
	servers    map[uint32]*Server
	serverIDs  []uint32

	serversRequests map[uint32]*atomic.Int32
}

func newMockCluster(t *testing.T, n int) *mockCluster {
	tmpDir := t.TempDir()

	serverIDs := make([]uint32, n)
	for i := 0; i < n; i++ {
		serverIDs[i] = uint32(i + 1)
	}

	rpcClient := newMockRaftClient()

	servers := make(map[uint32]*Server, n)
	for i := 0; i < n; i++ {
		server, err := NewServer(serverIDs[i], serverIDs, tmpDir, rpcClient)
		if err != nil {
			t.Fatalf("Failed to create server %d: %v", i, err)
		}

		servers[serverIDs[i]] = server
		rpcClient.servers[serverIDs[i]] = server
	}

	return &mockCluster{
		t:               t,
		servers:         servers,
		serverIDs:       serverIDs,
		mockClient:      rpcClient,
		serversRequests: make(map[uint32]*atomic.Int32),
	}
}

func (c *mockCluster) startAll() {
	for _, server := range c.servers {
		go server.Start()
	}
}

func (c *mockCluster) getLeader() *Server {
	for _, server := range c.servers {
		server.mx.RLock()
		isLeader := server.state == Leader
		server.mx.RUnlock()

		if isLeader {
			return server
		}
	}

	return nil
}

func (c *mockCluster) countByState(state State) int {
	count := 0
	for _, server := range c.servers {
		server.mx.RLock()
		if server.state == state {
			count++
		}
		server.mx.RUnlock()
	}
	return count
}

func (c *mockCluster) waitForLeader(timeout time.Duration) (*Server, error) {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		leader := c.getLeader()
		if leader != nil {
			return leader, nil
		}

		time.Sleep(50 * time.Millisecond)
	}

	return nil, fmt.Errorf("no leader elected within timeout")
}

func (c *mockCluster) waitForCondition(timeout time.Duration, condition func() bool) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("condition not met within timeout")
}

func (c *mockCluster) shutdown() {
	for _, server := range c.servers {
		server.Shutdown()
	}
}

func (c *mockCluster) getServerStateAndTerm(id uint32) (State, uint32) {
	server := c.servers[id]
	server.mx.RLock()
	defer server.mx.RUnlock()
	return server.state, server.persistentState.currentTerm
}

type mockRaftClient struct {
	mx sync.RWMutex

	servers map[uint32]*Server

	disconnected   map[uint32]bool
	voteRequests   map[uint32][]*RequestVoteRequest
	appendRequests map[uint32][]*AppendEntriesRequest

	requestVoteCalls   atomic.Int32
	appendEntriesCalls atomic.Int32
}

func newMockRaftClient() *mockRaftClient {
	return &mockRaftClient{
		servers:        make(map[uint32]*Server),
		disconnected:   make(map[uint32]bool),
		voteRequests:   make(map[uint32][]*RequestVoteRequest),
		appendRequests: make(map[uint32][]*AppendEntriesRequest),
	}
}

func (c *mockRaftClient) sendRequestVote(serverID uint32, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	c.requestVoteCalls.Add(1)

	c.mx.Lock()
	c.voteRequests[serverID] = append(c.voteRequests[serverID], req)

	if c.disconnected[serverID] {
		c.mx.Unlock()
		return nil, fmt.Errorf("server %d disconnected", serverID)
	}

	server := c.servers[serverID]
	c.mx.Unlock()

	if server == nil {
		return nil, fmt.Errorf("server %d not found", serverID)
	}

	return server.HandleRequestVote(req), nil
}

func (c *mockRaftClient) sendAppendEntries(serverID uint32, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	c.appendEntriesCalls.Add(1)

	c.mx.Lock()
	c.appendRequests[serverID] = append(c.appendRequests[serverID], req)

	if c.disconnected[serverID] {
		c.mx.Unlock()
		return nil, fmt.Errorf("server %d  disconnected", serverID)
	}

	server := c.servers[serverID]
	c.mx.Unlock()

	if server == nil {
		return nil, fmt.Errorf("server %d not found", serverID)
	}

	return server.HandleAppendEntries(req), nil
}

func (c *mockRaftClient) disconnect(serverID uint32) {
	c.mx.Lock()
	defer c.mx.Unlock()
	c.disconnected[serverID] = true
}

func (c *mockRaftClient) reconnect(serverID uint32) {
	c.mx.Lock()
	defer c.mx.Unlock()
	delete(c.disconnected, serverID)
}

func TestTest(t *testing.T) {
	for i := 0; i < 10; i++ {
		fmt.Println(time.Duration(150+rand.Intn(151)) * time.Millisecond)
	}
}

func (c *mockRaftClient) getVoteRequestsTo(serverID uint32) []*RequestVoteRequest {
	c.mx.RLock()
	defer c.mx.RUnlock()
	return c.voteRequests[serverID]
}

func TestServerElection_SingleServerBecomesCandidate(t *testing.T) {
	cluster := newMockCluster(t, 1)
	defer cluster.shutdown()

	serverID := cluster.serverIDs[0]
	server := cluster.servers[serverID]

	state, term := cluster.getServerStateAndTerm(server.ID)
	require.Equal(t, State(Follower), state, "Server state should be Follower")
	require.Equal(t, uint32(0), term, "Term should be 0")

	// start election time when the server starts
	server.Start()

	time.Sleep(350 * time.Millisecond)

	state, term = cluster.getServerStateAndTerm(server.ID)
	require.Equal(t, State(Candidate), state, "Server state should be Candidate")
	require.Equal(t, uint32(1), term, "Term should be 1 after starting an election")

	server.mx.Lock()
	votedFor := server.persistentState.votedFor
	server.mx.Unlock()

	require.Equal(t, serverID, votedFor, "Expected server to vote for itself")
}

func TestServerElection_FiveServers_OneLeader_WithNetworkPartition(t *testing.T) {
	numOfServers := 5
	cluster := newMockCluster(t, numOfServers)
	defer cluster.shutdown()

	cluster.startAll()

	t.Logf("Wait for leader election...")
	leader, err := cluster.waitForLeader(3 * time.Second)
	require.NoError(t, err, "Failed to elect leader")

	t.Logf("Leader elected: %d", leader.ID)
	selectedLeaderState, selectedLeaderTerm := cluster.getServerStateAndTerm(leader.ID)
	require.Equal(t, State(Leader), selectedLeaderState, "State should be Leader")
	require.True(t, selectedLeaderTerm > 0, "Term should be 1 after starting an election")

	require.Equal(t, 1, cluster.countByState(Leader), "Expected only 1 leader")

	// verify all servers have updated to the same term
	for i, server := range cluster.servers {
		state, term := cluster.getServerStateAndTerm(server.ID)
		t.Logf("Server %d: state=%d term=%d", i, state, term)

		// all servers should be on the same term or close to it
		if term < selectedLeaderTerm-1 || term > selectedLeaderTerm {
			t.Errorf("Server %d term (%d) too different from leader term (%d)", i, term, selectedLeaderTerm)
		}
	}

	// verify cluster counted at least 4 voteRequest calls for 5 serves
	// from the perspective of leader server
	expectedNumOfFollowers := numOfServers - 1
	numOfFollowers := cluster.countByState(Follower)

	require.Equal(t, expectedNumOfFollowers, numOfFollowers,
		"Expected %d followers, got %d", expectedNumOfFollowers, numOfFollowers)

	rpcClient, _ := cluster.mockClient.(*mockRaftClient)
	totalVoteRequests := int(rpcClient.requestVoteCalls.Load())
	t.Logf("Total vote requests: %d", totalVoteRequests)

	require.False(t, totalVoteRequests < 4,
		"Expected at least 4 VoteRequest calls, got %d", totalVoteRequests)

	t.Logf("Disconnect 2 servers...")
	rpcClient.disconnect(cluster.serverIDs[1])
	rpcClient.disconnect(cluster.serverIDs[2])

	// wait for election timeout
	newLeader, err := cluster.waitForLeader(3 * time.Second)
	require.NoError(t, err, "Failed to elect leader")
	t.Logf("New leader elected: %d", newLeader.ID)

	t.Logf("Restore 2 servers...")
	rpcClient.reconnect(cluster.serverIDs[1])
	rpcClient.reconnect(cluster.serverIDs[2])

	// wait for the cluster to stabilize
	time.Sleep(1 * time.Second)

	leader, err = cluster.waitForLeader(2 * time.Second)
	require.NoError(t, err)

	// check that we still have only 1 leader
	numOfLeaders := cluster.countByState(Leader)
	require.Equal(t, 1, numOfLeaders, "Should be 1 leader, got %d", numOfLeaders)
}

func TestServerReplication_EndToEnd(t *testing.T) {
	cluster := newMockCluster(t, 5)
	defer cluster.shutdown()

	cluster.startAll()

	t.Log("Waiting for leader election...")
	leader, err := cluster.waitForLeader(3 * time.Second)
	require.NoError(t, err, "Failed to elect leader")
	t.Logf("Leader elected: server %d", leader.ID)

	cmd := []byte("SET key1 value1")

	t.Log("Sending command to leader...")
	err = leader.HandleCommand(cmd)
	require.NoError(t, err, "Failed to append command")

	// the command should be replicated and committed on all servers
	t.Log("Waiting for log replication...")
	err = cluster.waitForCondition(5*time.Second, func() bool {
		// check that all servers have the log entry
		for _, server := range cluster.servers {
			server.mx.RLock()
			logLen := len(server.persistentState.log)
			server.mx.RUnlock()

			if logLen < 1 {
				return false
			}
		}
		return true
	})
	require.NoError(t, err, "Log not replicated to all servers within timeout")

	// verify log entry on all servers
	for _, server := range cluster.servers {
		server.mx.RLock()
		logs := server.persistentState.log
		server.mx.RUnlock()

		require.Len(t, logs, 1, "Server %d should have 1 log entry", server.ID)
		require.Equal(t, cmd, logs[0].Command, "Server %d has incorrect command", server.ID)
		require.Equal(t, uint32(1), logs[0].Index, "Server %d has incorrect log index", server.ID)

		t.Logf("Server %d: log[0] = {Index: %d, Term: %d, Command: %s}",
			server.ID, logs[0].Index, logs[0].Term, string(logs[0].Command))
	}

	// wait for commits to propagate
	t.Log("Waiting for commits to propagate...")
	err = cluster.waitForCondition(3*time.Second, func() bool {
		for _, server := range cluster.servers {
			server.mx.RLock()
			committed := server.volatileState.commitedIndex
			server.mx.RUnlock()

			if committed < 1 {
				return false
			}
		}
		return true
	})
	require.NoError(t, err, "Commits not propagated to all servers within timeout")

	// verify commit index on all servers
	t.Log("Verifying commit indices...")
	for _, server := range cluster.servers {
		server.mx.RLock()
		commitIndex := server.volatileState.commitedIndex
		lastApplied := server.volatileState.lastApplied
		server.mx.RUnlock()

		require.GreaterOrEqual(t, commitIndex, uint32(1),
			"Server %d should have committed index >= 1", server.ID)
		require.GreaterOrEqual(t, lastApplied, uint32(1),
			"Server %d should have applied index >= 1", server.ID)

		t.Logf("Server %d: commitIndex=%d, lastApplied=%d",
			server.ID, commitIndex, lastApplied)
	}
}
