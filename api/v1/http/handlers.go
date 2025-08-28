package http_endpoint

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/lynnphayu/dag-runner/internal/services/manager"
	"github.com/lynnphayu/dag-runner/internal/services/runner"
)

type RunnerHandler struct {
	runnerService  *runner.RunnerService
	managerService *manager.ManagerService
}

func NewRunnerHandler(runnerService *runner.RunnerService, managerService *manager.ManagerService) *RunnerHandler {

	return &RunnerHandler{
		runnerService:  runnerService,
		managerService: managerService,
	}
}

func (h *RunnerHandler) GetTableNames(w http.ResponseWriter, r *http.Request) {

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

func (h *RunnerHandler) GetColumns(w http.ResponseWriter, r *http.Request) {
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

func (h *RunnerHandler) RegisterFlowRoute(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	graphId := vars["id"]
	h.runnerService.RegisterFlowRoute(graphId, r.Context().Value("router").(*mux.Router))
}

func (h *RunnerHandler) RegisterRoutes(router *mux.Router) {
	router.HandleFunc("/v1/tables", h.GetTableNames).Methods("GET")
	router.HandleFunc("/v1/tables/{name}", h.GetColumns).Methods("GET")

	router.HandleFunc("/v1/dags/{id}/register", h.RegisterFlowRoute).Methods("GET")
}
