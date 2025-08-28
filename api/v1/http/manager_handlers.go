package http_endpoint

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/lynnphayu/dag-runner/internal/services/manager"
	"github.com/lynnphayu/dag-runner/pkg/dag"
)

type ManagerHandler struct {
	managerService *manager.ManagerService
}

func NewManagerHandler(managerService *manager.ManagerService) *ManagerHandler {
	return &ManagerHandler{
		managerService: managerService,
	}
}

func (h *ManagerHandler) SaveAdapter(w http.ResponseWriter, r *http.Request) {
	var adapter dag.Adapter[any]
	if err := json.NewDecoder(r.Body).Decode(&adapter); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if err := h.managerService.SaveAdapter(&adapter); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func (h *ManagerHandler) SaveDAG(w http.ResponseWriter, r *http.Request) {
	var g dag.Graph[*dag.Action]
	if err := json.NewDecoder(r.Body).Decode(&g); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if err := h.managerService.SaveDAG(&g); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func (h *ManagerHandler) GetDAG(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	d, err := h.managerService.GetDAG(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(d)
}

func (h *ManagerHandler) ListDAGs(w http.ResponseWriter, r *http.Request) {
	dags, err := h.managerService.ListDAGs()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(dags)
}

func (h *ManagerHandler) DeleteDAG(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	if err := h.managerService.DeleteDAG(id); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *ManagerHandler) UpdateDAG(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	var g dag.Graph[*dag.Action]
	if err := json.NewDecoder(r.Body).Decode(&g); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	g.ID = id
	result, err := h.managerService.UpdateDAG(&g)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func (h *ManagerHandler) RegisterRoutes(router *mux.Router) {
	router.HandleFunc("/v1/dags", h.SaveDAG).Methods("POST")
	router.HandleFunc("/v1/dags", h.ListDAGs).Methods("GET")
	router.HandleFunc("/v1/dags/{id}", h.GetDAG).Methods("GET")
	router.HandleFunc("/v1/dags/{id}", h.UpdateDAG).Methods("PUT")
	router.HandleFunc("/v1/dags/{id}", h.DeleteDAG).Methods("DELETE")

	router.HandleFunc("/v1/adapters", h.SaveAdapter).Methods("POST")
}
