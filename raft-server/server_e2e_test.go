package server

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	docker_network "github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type testRaftNode struct {
	id        uint32
	container testcontainers.Container
	hostPort  string
	networkIP string
}

func (n *testRaftNode) isLeader() (bool, error) {
	resp, err := http.Get(fmt.Sprintf("http://%s/health", n.hostPort))
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("health check failed with status %d", resp.StatusCode)
	}

	var healthResp struct {
		Term     uint32 `json:"term"`
		IsLeader bool   `json:"isLeader"`
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}

	if err = json.Unmarshal(body, &healthResp); err != nil {
		return false, nil
	}

	return healthResp.IsLeader, nil
}

func (n *testRaftNode) sendCommand(cmd []byte) error {
	resp, err := http.Post(
		fmt.Sprintf("http://%s/command", n.hostPort),
		"application/json",
		strings.NewReader(string(cmd)),
	)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("command failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

func (n *testRaftNode) getLogs() ([]map[string]interface{}, error) {
	resp, err := http.Get(fmt.Sprintf("http://%s/logs", n.hostPort))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get logs failed with status %d", resp.StatusCode)
	}

	var logs []map[string]interface{}
	if err = json.NewDecoder(resp.Body).Decode(&logs); err != nil {
		return nil, err
	}

	return logs, nil
}

type testRaftCluster struct {
	t   *testing.T
	ctx context.Context

	nodes      []*testRaftNode
	network    *testcontainers.DockerNetwork
	networkIPs []string
}

func newE2eTestCluster(t *testing.T, ctx context.Context, nodesCount int) (*testRaftCluster, error) {
	testDockerNetwork, err := docker_network.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start docker network: %v", err)
	}

	cluster := &testRaftCluster{
		t:       t,
		ctx:     ctx,
		nodes:   make([]*testRaftNode, nodesCount),
		network: testDockerNetwork,
	}

	for id := 1; id <= nodesCount; id++ {
		node, _err := cluster.startNode(uint32(id), nodesCount)
		if _err != nil {
			cluster.shutdown()
			return nil, fmt.Errorf("failed to start node %d: %v", id, _err)
		}

		cluster.nodes = append(cluster.nodes, node)
	}

	cluster.t.Logf("Cluster created with %d nodes", nodesCount)
	for _, node := range cluster.nodes {
		t.Logf(" Node %d http://%s", node.id, node.hostPort)
	}

	return cluster, nil
}

func (c *testRaftCluster) startNode(peerID uint32, nodesCount int) (*testRaftNode, error) {
	peers := make([]string, nodesCount)
	// todo: uint32 -> uuid for id
	for id := 1; id <= nodesCount; id++ {
		if id != int(peerID) {
			peers = append(peers, fmt.Sprintf("raft-node-%d:8000", id))
		}
	}

	peersList := strings.Join(peers, ",")

	req := testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "casual-raft:latest",
			Name:         fmt.Sprintf("raft-node-%d", peerID),
			ExposedPorts: []string{"8000/tcp"},
			Env: map[string]string{
				"RAFT_ID":    fmt.Sprintf("%d", peerID),
				"RAFT_PORT":  "8000",
				"RAFT_PEERS": peersList,
			},
			Networks: []string{c.network.Name},
			Cmd: []string{
				"--id", fmt.Sprintf("%d", peerID),
				"--port", "8000",
				"--peers", peersList,
				"--data", "/data",
			},
			WaitingFor: wait.ForHTTP("/health").
				WithPort("8000/tcp").
				WithStartupTimeout(30 * time.Second),
		},
		Started: true,
	}

	container, err := testcontainers.GenericContainer(c.ctx, req)
	if err != nil {
		return nil, err
	}

	hostPort, err := container.MappedPort(c.ctx, "8000")
	if err != nil {
		_ = container.Terminate(c.ctx)
		return nil, err
	}

	host, err := container.Host(c.ctx)
	if err != nil {
		_ = container.Terminate(c.ctx)
		return nil, err
	}

	networkIP, err := container.ContainerIP(c.ctx)
	if err != nil {
		_ = container.Terminate(c.ctx)
		return nil, err
	}

	node := &testRaftNode{
		id:        peerID,
		container: container,
		hostPort:  fmt.Sprintf("%s:%s", host, hostPort.Port()),
		networkIP: networkIP,
	}

	return node, nil
}

func (c *testRaftCluster) shutdown() {
	for _, node := range c.nodes {
		if node.container != nil {
			_ = node.container.Terminate(c.ctx)
		}
	}

	if c.network != nil {
		_ = c.network.Remove(c.ctx)
	}
}

func (c *testRaftCluster) getLeader() (*testRaftNode, error) {
	for _, node := range c.nodes {
		isLeader, err := node.isLeader()
		if err != nil {
			c.t.Logf("Error checking if node %d is leader: %v", node.id, err)
			continue
		}

		if isLeader {
			return node, nil
		}
	}

	return nil, fmt.Errorf("no leader found")
}

func (c *testRaftCluster) waitForLeader(t *testing.T, timeout time.Duration) (*testRaftNode, error) {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		leader, err := c.getLeader()
		if err == nil && leader != nil {
			t.Logf("Leader elected: node %d", leader.id)
			return leader, nil
		}

		time.Sleep(100 * time.Millisecond)
	}

	return nil, fmt.Errorf("no leader elected within timeout")
}

func (c *testRaftCluster) stopNode(t *testing.T, nodeID uint32) error {
	for _, node := range c.nodes {
		if node.id == nodeID {
			t.Logf("Stopping node %d", nodeID)
			return node.container.Stop(c.ctx, nil)
		}
	}
	return fmt.Errorf("node %d not found", nodeID)
}

func TestE2E(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E tests in short mode")
	}

	ctx := context.Background()
	nodesCount := 5

	cluster, err := newE2eTestCluster(t, ctx, nodesCount)
	require.NoError(t, err)
	defer cluster.shutdown()

	t.Logf("Cluster created with %d nodes", nodesCount)
	t.Logf("Waiting for leader election...")

	leader, err := cluster.waitForLeader(t, 10*time.Second)
	require.NoError(t, err)
	require.NotNil(t, leader)

	t.Logf("Leader elected: node %d", leader.id)

	leaderCount := 0
	for _, node := range cluster.nodes {
		isLeader, _err := node.isLeader()
		if _err == nil && isLeader {
			leaderCount++
		}
	}

	// verify only one leader exists
	require.Equal(t, 1, leaderCount)

	// send a command to the leader
	cmd := []byte(`{"key": "test-key", "value": "test-value"}"`)
	t.Logf("Sending command to the leader: %s", string(cmd))

	err = leader.sendCommand(cmd)
	require.NoError(t, err)

	// wait for replications
	time.Sleep(3 * time.Second)

	for _, node := range cluster.nodes {
		logs, _err := node.getLogs()
		if _err != nil {
			t.Logf("Error getting logs: %v", _err)
			continue
		}

		require.GreaterOrEqual(t, 1, len(logs))
	}

	t.Logf("Command successfully replicated to all nodes")
}
