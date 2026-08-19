package discovery

import (
	"fmt"

	"github.com/hashicorp/memberlist"
)

// Membership wraps the HashiCorp memberlist to manage our cluster.
type Membership struct {
	list *memberlist.Memberlist
}

// New creates a new gossip node on a specific port.
func New(bindPort int) (*Membership, error) {
	// Use the default configuration for a local network
	config := memberlist.DefaultLocalConfig()

	// Set the port this node will use to gossip (UDP/TCP)
	// Note: It's good practice to use a separate port for gossip than HTTP.
	// For simplicity, we'll just add 10000 to the HTTP port (e.g., HTTP 8080 -> Gossip 18080)
	config.BindPort = bindPort + 10000

	// Give the node a unique name
	config.Name = fmt.Sprintf("node-%d", bindPort)

	list, err := memberlist.Create(config)
	if err != nil {
		return nil, err
	}

	return &Membership{list: list}, nil
}

// Join connects this node to an existing cluster.
func (m *Membership) Join(seedAddress string) error {
	_, err := m.list.Join([]string{seedAddress})
	return err
}

// GetMembers returns a list of all healthy nodes in the cluster.
func (m *Membership) GetMembers() []string {
	var activeNodes []string
	for _, member := range m.list.Members() {
		activeNodes = append(activeNodes, member.Name)
	}
	return activeNodes
}
