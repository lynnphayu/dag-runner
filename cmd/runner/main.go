package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/lynnphayu/swift/dagflow/internal/dag"
)

func main() {
	// Get configuration from environment variables with defaults
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		panic("DATABASE_URL environment variable is not set")
	}

	server, err := dag.NewServer(connStr)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	http.HandleFunc("/execute", server.ExecuteHandler)
	log.Printf("Starting server on port %s", port)
	if err := http.ListenAndServe(fmt.Sprintf(":%s", port), nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
