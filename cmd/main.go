package main

import (
	"flag"
	"fmt"
	raftserver "github.com/Konstantsiy/casual-raft/raft-server"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func main() {
	var (
		id      = flag.Uint("id", 0, "ID of this server")
		port    = flag.String("port", "8000", "HTTP port")
		peers   = flag.String("peers", "", "Comma separated list of peers (e.g., localhost:8001,localhost:8002)")
		dataDir = flag.String("data", "./data", "Data directory for persistent state")
	)

	flag.Parse()

	if *id == 0 {
		log.Fatal("Server ID mus be provided")
	}

	if *peers == "" {
		log.Fatal("Peers mus be provided")
	}

	peersAddresses := strings.Split(*peers, ",")
	peersIDs := make([]uint32, len(peersAddresses))
	for i := range peersAddresses {
		peersIDs[i] = uint32(i)
	}

	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	client := raftserver.NewRaftClient(peersAddresses)
	server, err := raftserver.NewServer(uint32(*id), peersIDs, *dataDir, client)
	if err != nil {
		log.Fatal("Failed to create server")
	}

	server.Shutdown()
	defer server.Shutdown()

	handler := raftserver.NewHTTPHandler(server)
	mux := http.NewServeMux()
	handler.RegisterHandlers(mux)

	// set healthcheck
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		term, isLeader := server.State()
		log.Printf("Health check: term=%d isLeader=%t", term, isLeader)
	})

	httpServer := &http.Server{Addr: fmt.Sprintf(":%s", *port), Handler: mux}

	go func() {
		log.Printf("Server %d listening on port %s", *id, *port)
		log.Fatal(httpServer.ListenAndServe())
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")
}
