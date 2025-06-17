package http_endpoint

import (
	"github.com/gorilla/mux"
)

func RegisterRoutes(router *mux.Router, runnerHandler *RunnerHandler, managerHandler *ManagerHandler) {
	router.HandleFunc("/v1/dags/{id}/execute", runnerHandler.ExecuteDAGByID).Methods("POST")
	router.HandleFunc("/v1/flows/execute", runnerHandler.ExecuteDAG).Methods("POST")
	router.HandleFunc("/v1/tables", runnerHandler.GetTableNames).Methods("GET")
	router.HandleFunc("/v1/tables/{name}", runnerHandler.GetColumns).Methods("GET")

	router.HandleFunc("/v1/dags", managerHandler.SaveDAG).Methods("POST")
	router.HandleFunc("/v1/dags", managerHandler.ListDAGs).Methods("GET")
	router.HandleFunc("/v1/dags/{id}", managerHandler.GetDAG).Methods("GET")
	router.HandleFunc("/v1/dags/{id}", managerHandler.UpdateDAG).Methods("PUT")
	router.HandleFunc("/v1/dags/{id}", managerHandler.DeleteDAG).Methods("DELETE")

}
