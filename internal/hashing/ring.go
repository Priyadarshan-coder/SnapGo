package hashing

import (
	"hash/crc32"
	"sort"
	"strconv"
)

// HashRing manages the consistent hashing ring.
type HashRing struct {
	replicas int               // Number of virtual nodes per physical server
	keys     []uint32          // A sorted list of all the hashes on the ring
	hashMap  map[uint32]string // Maps a hash number back to the actual node name
}

// New creates a new HashRing.
// 'replicas' dictates how many virtual nodes we create to ensure even data distribution.
func New(replicas int) *HashRing {
	return &HashRing{
		replicas: replicas,
		hashMap:  make(map[uint32]string),
	}
}

// AddNodes takes the names of our active servers (e.g. "node-8080", "node-8081")
// and places them onto the ring.
func (r *HashRing) AddNodes(nodeNames ...string) {
	for _, node := range nodeNames {
		// Create multiple "virtual" spots on the ring for each physical node
		for i := 0; i < r.replicas; i++ {
			// Create a unique string like "0node-8080", "1node-8080"
			virtualNodeName := strconv.Itoa(i) + node

			// Turn that string into a number (the hash)
			hash := crc32.ChecksumIEEE([]byte(virtualNodeName))

			// Save the hash to our slice and map it back to the real node name
			r.keys = append(r.keys, hash)
			r.hashMap[hash] = node
		}
	}

	// The magic of the ring: sort the hashes from smallest to largest!
	sort.Slice(r.keys, func(i, j int) bool {
		return r.keys[i] < r.keys[j]
	})
}

// GetNode takes a user's key (e.g. "hero") and figures out which node should hold it.
func (r *HashRing) GetNode(key string) string {
	if len(r.keys) == 0 {
		return ""
	}

	// Turn the user's key into a number
	hash := crc32.ChecksumIEEE([]byte(key))

	// Binary Search: Find the first virtual node hash that is >= our key's hash
	idx := sort.Search(len(r.keys), func(i int) bool {
		return r.keys[i] >= hash
	})

	// If the key's hash is larger than the very last node on our ring,
	// we wrap around back to the very first node (index 0).
	if idx == len(r.keys) {
		idx = 0
	}

	// Return the name of the actual physical node (e.g. "node-8081")
	return r.hashMap[r.keys[idx]]
}
