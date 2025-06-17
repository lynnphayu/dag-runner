package http_endpoint

import (
	"encoding/json"
	"fmt"
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

func (h *ManagerHandler) SaveDAG(w http.ResponseWriter, r *http.Request) {
	var dag dag.DAG
	if err := json.NewDecoder(r.Body).Decode(&dag); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if err := h.managerService.SaveDAG(&dag); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func (h *ManagerHandler) GetDAG(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	dag, err := h.managerService.GetDAG(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(dag)
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

	var dag dag.DAG
	if err := json.NewDecoder(r.Body).Decode(&dag); err != nil {
		fmt.Println(err)
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	dag.ID = id
	if result, err := h.managerService.UpdateDAG(&dag); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	} else {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	}

}
