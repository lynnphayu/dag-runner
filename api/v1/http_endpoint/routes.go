package http_endpoint

import (
	"github.com/gorilla/mux"
)

func RegisterRoutes(router *mux.Router, handler *Handler) {
	router.HandleFunc("/v1/flows/execute", handler.ExecuteDAG).Methods("POST")
	router.HandleFunc("/v1/tables", handler.GetTableNames).Methods("GET")
	router.HandleFunc("/v1/tables/{name}", handler.GetColumns).Methods("GET")
}
