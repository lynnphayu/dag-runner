package http_endpoint

import (
	"log/slog"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/lynnphayu/dag-runner/internal/services/manager"
	"github.com/lynnphayu/dag-runner/internal/services/runner"
)

type RunnerHandler struct {
	runnerService  *runner.RunnerService
	managerService *manager.ManagerService
	logger         *slog.Logger
}

func NewRunnerHandler(runnerService *runner.RunnerService, managerService *manager.ManagerService) *RunnerHandler {

	return &RunnerHandler{
		runnerService:  runnerService,
		managerService: managerService,
		logger:         slog.Default().With("component", "runner_handler"),
	}
}

func (h *RunnerHandler) GetTableNames(w http.ResponseWriter, r *http.Request) {
	result, err := h.runnerService.GetTableNames()
	if err != nil {
		writeInternalError(w, r, h.logger, err)
		return
	}

	writeOK(w, map[string]interface{}{
		"data": result,
	})
}

func (h *RunnerHandler) GetColumns(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	tableName := vars["name"]

	result, err := h.runnerService.GetColumns(tableName)
	if err != nil {
		writeInternalError(w, r, h.logger.With("table_name", tableName), err)
		return
	}

	writeOK(w, map[string]interface{}{
		"data": result,
	})
}

func (h *RunnerHandler) registerFlowRouteHandler(router *mux.Router) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		graphId := vars["id"]

		if err := h.runnerService.RegisterFlowRoute(graphId, router); err != nil {
			writeInternalError(w, r, h.logger.With("graph_id", graphId), err)
			return
		}

		h.logger.Info(
			"flow route registered",
			"graph_id", graphId,
			"method", r.Method,
			"path", r.URL.Path,
		)

		writeNoContent(w)
	}
}

func (h *RunnerHandler) RegisterRoutes(router *mux.Router) {
	router.HandleFunc("/v1/tables", h.GetTableNames).Methods("GET")
	router.HandleFunc("/v1/tables/{name}", h.GetColumns).Methods("GET")

	router.HandleFunc("/v1/dags/{id}/register", h.registerFlowRouteHandler(router)).Methods("POST")
}
