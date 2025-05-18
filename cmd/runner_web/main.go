package main

import (
	"log"
	"net/http"
	"os"

	"github.com/gorilla/mux"
	"github.com/lynnphayu/dag-runner/api/v1/http_endpoint"
	"github.com/lynnphayu/dag-runner/internal/services/runner"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		log.Fatalf("missing DATABASE_URL environment variable")
	}
	runnerService := runner.NewRunnerService(connStr)

	router := mux.NewRouter()
	server := http_endpoint.NewHandler(runnerService)
	http_endpoint.RegisterRoutes(router, server)
	log.Printf("Server is running on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, router))
}
