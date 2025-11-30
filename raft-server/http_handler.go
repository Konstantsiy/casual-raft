package server

import (
	"encoding/json"
	"net/http"
)

type AppendEntriesRequest struct {
	Term         uint32     // leader's term
	LeaderID     uint32     // leader's ID
	PrevLogIndex uint32     // index of log entry immediately preceding new ones
	PrevLogTerm  uint32     // term of prevLogIndex entry
	Entries      []logEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit uint32     // leader's commitIndex
}

type AppendEntriesResponse struct {
	Term    uint32 // currentTerm, for leader to update itself
	Success bool   // true if follower contained entry matching prevLogIndex and prevLogTerm
}

type RequestVoteRequest struct {
	Term         uint32 // candidate's term
	CandidateID  uint32 // candidate requesting votes
	LastLogIndex uint32 // index of candidate's last log entry
	LastLogTerm  uint32 // term of candidate's last log entry
}

type RequestVoteResponse struct {
	Term        uint32 // currentTerm, for a candidate to update itself
	VoteGranted bool   // true means a candidate received a vote
}

type HTTPHandler struct {
	server *Server
}

func NewHTTPHandler(server *Server) *HTTPHandler {
	return &HTTPHandler{server: server}
}

func (h *HTTPHandler) RegisterHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/append_entries", h.handleAppendEntries)
	mux.HandleFunc("/request_vote", h.handleRequestVote)
	mux.HandleFunc("/command", h.handleCommand)
}

func (h *HTTPHandler) handleCommand(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var cmd []byte
	if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := h.server.HandleCommand(cmd); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *HTTPHandler) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req AppendEntriesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := h.server.HandleAppendEntries(&req)

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (h *HTTPHandler) handleRequestVote(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req RequestVoteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := h.server.HandleRequestVote(&req)

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
