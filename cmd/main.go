package main

import (
	"flag"
	raftserver "github.com/Konstantsiy/casual-raft/raft-server"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	configPath := flag.String("config", "./config.yaml", "Path to configuration file")
	flag.Parse()

	cfg, err := raftserver.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	var (
		peersAddresses = cfg.GetPeers()
		peersIDs       = cfg.GetPeerIDs()
	)

	client := raftserver.NewRaftClient(peersAddresses)

	server, err := raftserver.NewServer(cfg.Node.ID, peersIDs, cfg.Node.DataDir, client)
	if err != nil {
		log.Fatalf("Failed to create server: %s", err.Error())
	}

	server.Start()
	defer server.Shutdown()

	handler := raftserver.NewHTTPHandler(server)
	mux := http.NewServeMux()
	handler.RegisterHandlers(mux)

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		term, isLeader := server.State()
		log.Printf("Health check: term=%d isLeader=%t", term, isLeader)
	})

	httpServer := &http.Server{
		Addr:    cfg.Node.Address,
		Handler: mux,
	}

	go func() {
		log.Printf("Server %d listening on port %s", cfg.Node.ID, cfg.Node.Address)
		log.Fatal(httpServer.ListenAndServe())
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")
}
