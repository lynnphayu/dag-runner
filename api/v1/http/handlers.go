package http_endpoint

import (
	"encoding/json"
	"io"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/lynnphayu/dag-runner/internal/services/manager"
	"github.com/lynnphayu/dag-runner/internal/services/runner"
	"github.com/lynnphayu/dag-runner/pkg/dag"
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

func (h *RunnerHandler) RegisterFlowRoute(graphId string, router *mux.Router) {
	executor, adapter, err := h.runnerService.GetHttpHandlerPreference(graphId)
	if err != nil {
		log.Fatalf("failed to get http handler preference: %v", err)
	}
	router.HandleFunc(adapter.Meta.Path, func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			log.Fatalf("failed to read body: %v", err)
		}
		var input map[string]interface{}
		err = json.Unmarshal(body, &input)
		if err != nil {
			log.Fatalf("failed to unmarshal body: %v", err)
		}
		resolvedInput := dag.ResolveValues(adapter.InputMap, map[string]interface{}{
			"headers": r.Header,
			"body":    input,
			"query":   r.URL.Query(),
		}, &dag.Context{}).(map[string]interface{})

		w.Header().Set("Content-Type", "application/json")
		resultMap, err := executor.Execute(resolvedInput)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(&map[string]interface{}{
				"error": err.Error(),
			})
			return
		}
		if resultMap[adapter.Meta.ResposeNode] != nil {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(resultMap[adapter.Meta.ResposeNode])
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{})
	}).Methods(string(adapter.Meta.Method))

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

func (h *RunnerHandler) RegisterRoutes(router *mux.Router) {
	router.HandleFunc("/v1/tables", h.GetTableNames).Methods("GET")
	router.HandleFunc("/v1/tables/{name}", h.GetColumns).Methods("GET")
	h.RegisterFlowRoute("7cbf3569-2cba-4e53-9c69-a9fa811be3b4", router)
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
