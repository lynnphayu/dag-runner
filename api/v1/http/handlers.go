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

// func (h *RunnerHandler) RegisterFlowRoute(graphId string, router *mux.Router) {
// 	executor, adapter, err := h.runnerService.GetHttpHandlerPreference(graphId)
// 	if err != nil {
// 		log.Fatalf("failed to get http handler preference: %v", err)
// 	}
// 	router.HandleFunc(adapter.Path, func(w http.ResponseWriter, r *http.Request) {
// 		body, err := io.ReadAll(r.Body)
// 		if err != nil {
// 			log.Fatalf("failed to read body: %v", err)
// 		}
// 		var input map[string]interface{}
// 		err = json.Unmarshal(body, &input)
// 		if err != nil {
// 			log.Fatalf("failed to unmarshal body: %v", err)
// 		}
// 		resolvedInput := dag.ResolveValues(adapter.GetInputMap(), map[string]interface{}{
// 			"headers": r.Header,
// 			"body":    input,
// 			"query":   r.URL.Query(),
// 		}, &dag.Context{}).(map[string]interface{})
// 		executor.Execute(resolvedInput)
// 	}).Methods(string(adapter.Method))

// }

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

func (h *RunnerHandler) RegisterRoutes(router *mux.Router) {
	router.HandleFunc("/v1/tables", h.GetTableNames).Methods("GET")
	router.HandleFunc("/v1/tables/{name}", h.GetColumns).Methods("GET")
	// h.RegisterFlowRoute("fb99612a-7ede-4778-8023-1a98bd5f0018", router)
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
