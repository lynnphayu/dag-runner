package http_endpoint

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/lynnphayu/dag-runner/internal/services/runner"
)

type Handler struct {
	runnerService *runner.RunnerService
}

func NewHandler(runnerService *runner.RunnerService) *Handler {
	return &Handler{
		runnerService,
	}
}

func (h *Handler) ExecuteDAG(w http.ResponseWriter, r *http.Request) {
	var request runner.ExecuteRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	result, err := h.runnerService.Execute(&request.DAG, request.Input)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func (h *Handler) GetTableNames(w http.ResponseWriter, r *http.Request) {

	result, err := h.runnerService.GetTableNames()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(&map[string]interface{}{
		"data": result,
	})
}

func (h *Handler) GetColumns(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	tableName := vars["name"]

	result, err := h.runnerService.GetColumns(tableName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(&map[string]interface{}{
		"data": result,
	})
}

// func (h *Handler) GetOperationStatus(w http.ResponseWriter, r *http.Request) {
// 	vars := mux.Vars(r)
// 	operationID := vars["operationId"]

// 	status, err := h.executor.GetStatus(operationID)
// 	if err != nil {
// 		http.Error(w, err.Error(), http.StatusInternalServerError)
// 		return
// 	}

// 	w.Header().Set("Content-Type", "application/json")
// 	json.NewEncoder(w).Encode(status)
// }
