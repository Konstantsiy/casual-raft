package server

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"io/ioutil"
)

type Config struct {
	Node    NodeConfig    `yaml:"node"`
	Cluster ClusterConfig `yaml:"cluster"`
}

type NodeConfig struct {
	ID      uint32 `yaml:"id"`
	Address string `yaml:"address"`
	DataDir string `yaml:"data_dir"`
}

type ClusterConfig struct {
	Peers []PeerConfig `yaml:"peers"`
}

type PeerConfig struct {
	ID      uint32 `yaml:"id"`
	Address string `yaml:"address"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &config, nil
}

func (c *Config) Validate() error {
	if c.Node.ID == 0 {
		return fmt.Errorf("node.id must be greater than 0")
	}

	if c.Node.Address == "" {
		return fmt.Errorf("node.address is required")
	}

	if c.Node.DataDir == "" {
		return fmt.Errorf("node.data_dir is required")
	}

	if len(c.Cluster.Peers) == 0 {
		return fmt.Errorf("cluster.peers must contain at least one peer")
	}

	found := false
	for _, peer := range c.Cluster.Peers {
		if peer.ID == c.Node.ID {
			found = true
			if peer.Address != c.Node.Address {
				return fmt.Errorf("node address mismatch: node.address=%s but peer address=%s",
					c.Node.Address, peer.Address)
			}
			break
		}
	}

	if !found {
		return fmt.Errorf("node.id=%d not found in cluster.peers", c.Node.ID)
	}

	uniqueIDs := make(map[uint32]bool)
	for _, peer := range c.Cluster.Peers {
		if uniqueIDs[peer.ID] {
			return fmt.Errorf("duplicate peer ID: %d", peer.ID)
		}
		uniqueIDs[peer.ID] = true
	}

	return nil
}

func (c *Config) GetPeers() map[uint32]string {
	var res = make(map[uint32]string, len(c.Cluster.Peers))
	for _, peer := range c.Cluster.Peers {
		res[peer.ID] = peer.Address
	}
	return res
}

func (c *Config) GetPeerIDs() []uint32 {
	ids := make([]uint32, len(c.Cluster.Peers))
	for i, peer := range c.Cluster.Peers {
		ids[i] = peer.ID
	}
	return ids
}

func (c *Config) GetPeerAddresses() []string {
	var res = make([]string, len(c.Cluster.Peers))
	for i, peer := range c.Cluster.Peers {
		res[i] = peer.Address
	}
	return res
}
