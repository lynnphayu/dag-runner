package main

import (
	"log"
	"net/http"
	"os"

	"github.com/gorilla/mux"
	"github.com/lynnphayu/dag-runner/api/v1/http_endpoint"
	"github.com/lynnphayu/dag-runner/internal/services/manager"
	"github.com/lynnphayu/dag-runner/internal/services/runner"
	"github.com/rs/cors"
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
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		log.Fatalf("missing MONGO_URI environment variable")
	}

	runnerService := runner.NewRunnerService(connStr)
	managerService := manager.NewManagerService(mongoURI)

	router := mux.NewRouter()
	runner := http_endpoint.NewRunnerHandler(runnerService, managerService)
	manager := http_endpoint.NewManagerHandler(managerService)

	http_endpoint.RegisterRoutes(router, runner, manager)

	// Configure CORS
	c := cors.New(cors.Options{
		AllowedOrigins:   []string{"*"}, // You should restrict this in production
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token"},
		ExposedHeaders:   []string{"Link"},
		AllowCredentials: true,
		MaxAge:           300, // Maximum value not ignored by any of major browsers
	})

	// Wrap router with CORS middleware
	handler := c.Handler(router)

	log.Printf("Server is running on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, handler))
}
