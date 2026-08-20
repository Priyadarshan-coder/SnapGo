package httpapi

import (
	"SnapGo/internal/cache"
	"SnapGo/internal/discovery"
	"SnapGo/internal/hashing"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
)

type Server struct {
	cache      *cache.Cache
	membership *discovery.Membership
	nodeName   string
}

// New initializes the HTTP Server with the cache, membership, and node name
func New(c *cache.Cache, m *discovery.Membership, port int) *Server {
	return &Server{
		cache:      c,
		membership: m,
		nodeName:   fmt.Sprintf("node-%d", port),
	}
}

// Start begins listening for HTTP requests on the specified port.
func (s *Server) Start(port int) error {
	address := fmt.Sprintf(":%d", port)

	// 1. Create a brand new, isolated multiplexer (router)
	mux := http.NewServeMux()

	mux.HandleFunc("/set", s.handleSet)
	mux.HandleFunc("/get", s.handleGet)

	fmt.Printf("Cache node running on port %d...\n", port)

	// 3. Pass our custom mux into the server instead of 'nil'
	return http.ListenAndServe(address, mux)
}

// handleSet processes incoming SET requests.
func (s *Server) handleSet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	val := r.URL.Query().Get("val")
	isReplica := r.URL.Query().Get("replica") == "true" // Check if it's internal traffic

	if key == "" || val == "" {
		http.Error(w, "Missing key or val", http.StatusBadRequest)
		return
	}

	// Parse the TTL (default to 0 if not provided)
	ttlStr := r.URL.Query().Get("ttl")
	ttl := 0
	if ttlStr != "" {
		parsed, err := strconv.Atoi(ttlStr)
		if err == nil {
			ttl = parsed
		}
	}
	if isReplica {
		// If another node proxied this to us, just save it locally and exit!
		s.cache.Set(key, val, ttl)
		fmt.Printf("Saved '%s' locally on %s (via Replica)\n", key, s.nodeName)
		w.WriteHeader(http.StatusOK)
		return
	}
	activeNodes := s.membership.GetMembers()
	ring := hashing.New(50)
	ring.AddNodes(activeNodes...)

	targetNodes := ring.GetNodes(key, 2)

	// Concurrency tools
	var wg sync.WaitGroup
	var mu sync.Mutex
	var successCount atomic.Int32

	// Loop through our targets and launch a goroutine for each one
	for _, targetNode := range targetNodes {
		wg.Add(1)

		go func(node string) {
			defer wg.Done()

			if node == s.nodeName {
				// Save locally
				s.cache.Set(key, val, ttl)
				fmt.Printf("Saved '%s' locally on %s with TTL %d\n", key, s.nodeName, ttl)

				mu.Lock()
				successCount.Add(1)
				mu.Unlock()
			} else {
				// Proxy over the network with the &replica=true flag!
				targetPort := node[5:]
				forwardURL := fmt.Sprintf("http://localhost:%s/set?key=%s&val=%s&ttl=%d&replica=true", targetPort, key, val, ttl)

				fmt.Printf("Replicating '%s' to backup node %s...\n", key, node)
				resp, err := http.Get(forwardURL)

				if err == nil && resp.StatusCode == http.StatusOK {
					mu.Lock()
					successCount.Add(1)
					mu.Unlock()
					resp.Body.Close()
				}
			}
		}(targetNode)
	}

	// Wait here until all goroutines call wg.Done()
	wg.Wait()

	if successCount.Load() > 0 {
		fmt.Fprintf(w, "Success! Key '%s' saved to %d node(s) concurrently.\n", key, successCount.Load())
	} else {
		http.Error(w, "Failed to write to any nodes", http.StatusInternalServerError)
	}
}

// handleGet processes incoming GET requests.
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	isInternal := r.URL.Query().Get("internal") == "true" // Check for internal proxy traffic

	if key == "" {
		http.Error(w, "Missing key", http.StatusBadRequest)
		return
	}
	if isInternal {
		// If another node proxied this to us, do NOT ask the hash ring. Just check local RAM.
		val, found := s.cache.Get(key)
		if !found {
			http.Error(w, "Key not found locally", http.StatusNotFound)
			return
		}
		fmt.Fprintf(w, "Value: %s (Served locally from %s)\n", val, s.nodeName)
		return
	}
	// 1. Build the Hash Ring with the currently active nodes
	activeNodes := s.membership.GetMembers()
	ring := hashing.New(50) // 50 virtual nodes for even distribution
	ring.AddNodes(activeNodes...)

	// 2. Ask the ring who owns this key
	targetNode := ring.GetNode(key)

	// 3. If THIS node owns the data, fetch it locally
	if targetNode == s.nodeName {
		val, found := s.cache.Get(key)
		if !found {
			http.Error(w, "Key not found locally", http.StatusNotFound)
			return
		}
		fmt.Fprintf(w, "Value: %s (Served locally from %s)\n", val, s.nodeName)
		return
	}

	// 4. PROXY: If another node owns it, forward the request!
	// Add the &internal=true flag to prevent GET loops!
	targetPort := targetNode[5:]
	forwardURL := fmt.Sprintf("http://localhost:%s/get?key=%s&internal=true", targetPort, key)

	fmt.Printf("Proxying GET request for '%s' to %s...\n", key, targetNode)

	// Make the HTTP call to the other node
	resp, err := http.Get(forwardURL)
	if err != nil {
		http.Error(w, "Failed to reach target node", http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	// 5. Copy the other node's response directly back to the user
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}
