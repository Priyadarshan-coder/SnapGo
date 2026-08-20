package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"SnapGo/internal/cache"
	"SnapGo/internal/discovery"
	"SnapGo/internal/httpapi"
)

func main() {
	// 1. Parse configuration
	port := flag.Int("port", 8080, "HTTP port for this node")
	joinAddr := flag.String("join", "", "Gossip address of a node to join (e.g., 127.0.0.1:18081)")
	flag.Parse()

	// 2. Initialize the core cache engine
	// Tell the background sweeper to run every 10 seconds
	myCache := cache.New(10 * time.Second)

	// 3. Initialize the Gossip Membership
	fmt.Println("Starting gossip protocol...")
	membership, err := discovery.New(*port)
	if err != nil {
		log.Fatalf("Failed to start gossip: %v", err)
	}

	// 4. If a join address was provided, join that cluster!
	if *joinAddr != "" {
		fmt.Printf("Joining cluster at %s...\n", *joinAddr)
		err := membership.Join(*joinAddr)
		if err != nil {
			log.Fatalf("Failed to join cluster: %v", err)
		}
	}

	// Just to prove it works, let's print the cluster size
	members := membership.GetMembers()
	fmt.Printf("Cluster size is currently: %d node(s)\n", len(members))

	// 5. Start the HTTP server
	server := httpapi.New(myCache, membership, *port)
	if err := server.Start(*port); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
