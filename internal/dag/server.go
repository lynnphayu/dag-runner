package dag

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/lynnphayu/swift/dagflow/internal/dag/domain"
	"github.com/lynnphayu/swift/dagflow/internal/dag/flow"
)

type Server struct {
	executor *flow.Executor
}

type ExecuteRequest struct {
	DAG   domain.DAG             `json:"dag"`
	Input map[string]interface{} `json:"input"`
}

type ExecuteResponse struct {
	Result interface{} `json:"result,omitempty"`
	Error  string      `json:"error,omitempty"`
}

func NewServer(connStr string) (*Server, error) {
	executor, err := flow.NewExecutor(connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to create executor: %w", err)
	}
	return &Server{executor: executor}, nil
}

func (s *Server) ExecuteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ExecuteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	result, err := s.executor.Execute(&req.DAG, req.Input)

	w.Header().Set("Content-Type", "application/json")
	response := ExecuteResponse{}

	if err != nil {
		response.Error = err.Error()
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		response.Result = result
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
		return
	}
}
