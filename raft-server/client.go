package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

type RaftClient interface {
	sendAppendEntries(peerID uint32, req *AppendEntriesRequest) (*AppendEntriesResponse, error)
	sendRequestVote(peerID uint32, req *RequestVoteRequest) (*RequestVoteResponse, error)
}

type raftClient struct {
	// peers represent the list of peer addresses, e.g. ["localhost:8001", "localhost:8002]
	peers map[uint32]string
	// todo: add grpc later
	httpClient *http.Client
}

func NewRaftClient(peers map[uint32]string) RaftClient {
	return &raftClient{
		peers: peers,
		httpClient: &http.Client{
			Timeout: RPCTimeout,
		},
	}
}

func (c *raftClient) sendAppendEntries(serverID uint32, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	if int(serverID) >= len(c.peers) {
		return nil, fmt.Errorf("invalid server ID: %d", serverID)
	}

	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("http://%s/append_entries", c.peers[serverID])

	resp, err := c.httpClient.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var body AppendEntriesResponse
	if err = json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, err
	}

	return &body, nil
}

func (c *raftClient) sendRequestVote(serverID uint32, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	if int(serverID) >= len(c.peers) {
		return nil, fmt.Errorf("invalid server ID: %d", serverID)
	}

	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("http://%s/request_vote", c.peers[serverID])

	resp, err := c.httpClient.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var body RequestVoteResponse
	if err = json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, err
	}

	return &body, nil
}

func (c *raftClient) discoverPeers(address string) (*PeersDiscoverResponse, error) {
	resp, err := c.httpClient.Get(fmt.Sprintf("http://%s/discover", address))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var body PeersDiscoverResponse
	if err = json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, err
	}

	return &body, nil
}
