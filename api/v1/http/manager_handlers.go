package http_endpoint

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/lynnphayu/dag-runner/internal/logging"
	"github.com/lynnphayu/dag-runner/internal/services/manager"
	"github.com/lynnphayu/dag-runner/internal/services/runner"
	"github.com/lynnphayu/dag-runner/pkg/dag"
)

type ManagerHandler struct {
	managerService *manager.ManagerService
	runnerService  *runner.RunnerService
	router         *mux.Router
	logger         *slog.Logger
}

func NewManagerHandler(managerService *manager.ManagerService, runnerService *runner.RunnerService, router *mux.Router) *ManagerHandler {
	return &ManagerHandler{
		managerService: managerService,
		runnerService:  runnerService,
		router:         router,
		logger:         slog.Default().With("component", "manager_handler"),
	}
}

func (h *ManagerHandler) SaveAdapter(w http.ResponseWriter, r *http.Request) {
	logger := logging.FromContext(r.Context(), h.logger)

	var adapter dag.Adapter[any]
	if err := json.NewDecoder(r.Body).Decode(&adapter); err != nil {
		logger.Warn("invalid adapter payload", "error", err)
		writeInvalidBodyError(w, r, logger, err)
		return
	}

	if err := h.managerService.SaveAdapter(&adapter); err != nil {
		logger.Error("failed to save adapter", "adapter_id", adapter.ID, "graph_id", adapter.GraphID, "error", err)
		writeInternalError(w, r, logger, err)
		return
	}

	logger.Info("adapter saved", "adapter_id", adapter.ID, "graph_id", adapter.GraphID)
	writeCreated(w, nil)
}

func (h *ManagerHandler) SaveDAG(w http.ResponseWriter, r *http.Request) {
	logger := logging.FromContext(r.Context(), h.logger)

	var g dag.Graph[*dag.Action, any]
	if err := json.NewDecoder(r.Body).Decode(&g); err != nil {
		logger.Warn("invalid dag payload", "error", err)
		writeInvalidBodyError(w, r, logger, err)
		return
	}

	if err := h.managerService.SaveDAG(&g); err != nil {
		logger.Error("failed to save dag", "dag_id", g.ID, "error", err)
		writeInternalError(w, r, logger, err)
		return
	}

	logger.Info("dag saved", "dag_id", g.ID)
	writeCreated(w, nil)
}

func (h *ManagerHandler) GetDAG(w http.ResponseWriter, r *http.Request) {
	logger := logging.FromContext(r.Context(), h.logger)

	vars := mux.Vars(r)
	id := vars["id"]

	d, err := h.managerService.GetDAG(id)
	if err != nil {
		logger.Error("failed to get dag", "dag_id", id, "error", err)
		writeInternalError(w, r, logger, err)
		return
	}

	logger.Info("dag retrieved", "dag_id", id)
	writeOK(w, d)
}

func (h *ManagerHandler) ListDAGs(w http.ResponseWriter, r *http.Request) {
	logger := logging.FromContext(r.Context(), h.logger)

	dags, err := h.managerService.ListDAGs()
	if err != nil {
		logger.Error("failed to list dags", "error", err)
		writeInternalError(w, r, logger, err)
		return
	}

	logger.Info("dags listed", "count", len(dags))
	writeOK(w, dags)
}

func (h *ManagerHandler) DeleteDAG(w http.ResponseWriter, r *http.Request) {
	logger := logging.FromContext(r.Context(), h.logger)

	vars := mux.Vars(r)
	id := vars["id"]

	if err := h.managerService.DeleteDAG(id); err != nil {
		logger.Error("failed to delete dag", "dag_id", id, "error", err)
		writeInternalError(w, r, logger, err)
		return
	}

	logger.Info("dag deleted", "dag_id", id)
	writeNoContent(w)
}

func (h *ManagerHandler) UpdateDAG(w http.ResponseWriter, r *http.Request) {
	logger := logging.FromContext(r.Context(), h.logger)

	vars := mux.Vars(r)
	id := vars["id"]

	var g dag.Graph[*dag.Action, any]
	if err := json.NewDecoder(r.Body).Decode(&g); err != nil {
		logger.Warn("invalid dag update payload", "dag_id", id, "error", err)
		writeInvalidBodyError(w, r, logger, err)
		return
	}

	g.ID = id
	result, err := h.managerService.UpdateDAG(&g)
	if err != nil {
		logger.Error("failed to update dag", "dag_id", id, "error", err)
		writeInternalError(w, r, logger, err)
		return
	}

	logger.Info("dag updated", "dag_id", id)
	writeOK(w, result)
}

func (h *ManagerHandler) PublishDAG(w http.ResponseWriter, r *http.Request) {
	logger := logging.FromContext(r.Context(), h.logger)

	vars := mux.Vars(r)
	id := vars["id"]

	newID, version, err := h.managerService.PublishDAG(id)
	if err != nil {
		logger.Error("failed to publish dag", "dag_id", id, "error", err)
		writeInternalError(w, r, logger, err)
		return
	}

	var url string
	if h.runnerService != nil && h.router != nil {
		if err := h.runnerService.RegisterFlowRoute(id, h.router); err != nil {
			wrappedErr := fmt.Errorf("failed to register adapters: %w", err)
			logger.Error("failed to register flow route after publish", "dag_id", id, "error", wrappedErr)
			writeInternalError(w, r, logger, wrappedErr)
			return
		}
		if _, adapter, err := h.runnerService.GetHTTPHandlerAdapter(id); err == nil {
			url = fmt.Sprintf("%s %s", adapter.Meta.Method, adapter.Meta.Path)
		} else {
			logger.Warn("published dag without resolved http adapter", "dag_id", id, "error", err)
		}
	}

	logger.Info("dag published", "dag_id", id, "published_id", newID, "version", version, "url", url)
	writeOK(w, map[string]interface{}{"id": newID, "version": version, "status": "published", "url": url})
}

func (h *ManagerHandler) ListDAGVersions(w http.ResponseWriter, r *http.Request) {
	logger := logging.FromContext(r.Context(), h.logger)

	vars := mux.Vars(r)
	id := vars["id"]

	versions, err := h.managerService.ListDAGVersions(id)
	if err != nil {
		logger.Error("failed to list dag versions", "dag_id", id, "error", err)
		writeInternalError(w, r, logger, err)
		return
	}

	logger.Info("dag versions listed", "dag_id", id, "count", len(versions))
	writeOK(w, versions)
}

func (h *ManagerHandler) GetAdapter(w http.ResponseWriter, r *http.Request) {
	logger := logging.FromContext(r.Context(), h.logger)

	vars := mux.Vars(r)
	id := vars["id"]

	adapter, err := h.managerService.GetAdapter(id)
	if err != nil {
		logger.Error("failed to get adapter", "adapter_id", id, "error", err)
		writeInternalError(w, r, logger, err)
		return
	}

	logger.Info("adapter retrieved", "adapter_id", id, "graph_id", adapter.GraphID)
	writeOK(w, adapter)
}

func (h *ManagerHandler) ListAdapters(w http.ResponseWriter, r *http.Request) {
	logger := logging.FromContext(r.Context(), h.logger)

	userID := r.URL.Query().Get("userId")
	graphID := r.URL.Query().Get("graphId")

	adapters, err := h.managerService.ListAdapters(userID, graphID)
	if err != nil {
		logger.Error("failed to list adapters", "user_id", userID, "graph_id", graphID, "error", err)
		writeInternalError(w, r, logger, err)
		return
	}

	logger.Info("adapters listed", "user_id", userID, "graph_id", graphID, "count", len(adapters))
	writeOK(w, adapters)
}

func (h *ManagerHandler) RegisterRoutes(router *mux.Router) {
	router.HandleFunc("/v1/dags", h.SaveDAG).Methods("POST")
	router.HandleFunc("/v1/dags", h.ListDAGs).Methods("GET")
	router.HandleFunc("/v1/dags/{id}", h.GetDAG).Methods("GET")
	router.HandleFunc("/v1/dags/{id}", h.UpdateDAG).Methods("PUT")
	router.HandleFunc("/v1/dags/{id}", h.DeleteDAG).Methods("DELETE")
	router.HandleFunc("/v1/dags/{id}/publish", h.PublishDAG).Methods("POST")
	router.HandleFunc("/v1/dags/{id}/versions", h.ListDAGVersions).Methods("GET")

	router.HandleFunc("/v1/adapters", h.SaveAdapter).Methods("POST")
	router.HandleFunc("/v1/adapters", h.ListAdapters).Methods("GET")
	router.HandleFunc("/v1/adapters/{id}", h.GetAdapter).Methods("GET")
}
