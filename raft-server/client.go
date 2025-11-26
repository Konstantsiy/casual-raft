package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type RaftClient struct {
	// peers represent the list of peer addresses, e.g. ["localhost:8001", "localhost:8002]
	peers []string
	// todo: add grpc later
	httpClient *http.Client
}

func NewRaftClient(peers []string) *RaftClient {
	return &RaftClient{
		peers: peers,
		httpClient: &http.Client{
			Timeout: 100 * time.Millisecond,
		},
	}
}

func (c *RaftClient) sendAppendEntries(serverID uint32, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	if int(serverID) >= len(c.peers) { // todo: change to UUID
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

	var res AppendEntriesResponse
	if err = json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}

	return &res, nil
}

func (c *RaftClient) sendRequestVote(serverID uint32, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	if int(serverID) >= len(c.peers) { // todo: change to UUID
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

	var res RequestVoteResponse
	if err = json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}

	return &res, nil
}
