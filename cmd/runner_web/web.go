package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/gorilla/mux"
	"github.com/joho/godotenv"
	http_service "github.com/lynnphayu/dag-runner/api/v1/http"
	"github.com/lynnphayu/dag-runner/internal/services/manager"
	"github.com/lynnphayu/dag-runner/internal/services/runner"
	"github.com/rs/cors"
)

func main() {
	root, error := os.Getwd()
	if error != nil {
		panic(fmt.Sprintf("Failed to get working directory: %s", error))
	}
	envPath := filepath.Join(root, ".env")
	godotenv.Load(envPath)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8888"
	}

	postgresURI := os.Getenv("DATABASE_URL")
	if postgresURI == "" {
		log.Fatalf("missing DATABASE_URL environment variable")
	}
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		log.Fatalf("missing MONGO_URI environment variable")
	}

	runnerService := runner.NewRunnerService(postgresURI, mongoURI)
	managerService := manager.NewManagerService(mongoURI)

	router := mux.NewRouter()
	runnerHandler := http_service.NewRunnerHandler(runnerService, managerService)
	managerHandler := http_service.NewManagerHandler(managerService, runnerService, router)

	runnerHandler.RegisterRoutes(router)
	managerHandler.RegisterRoutes(router)

	if err := runnerService.RegisterAllPublishedFlowRoutes(router); err != nil {
		log.Fatalf("failed to pre-register published DAG routes: %v", err)
	}

	router.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}).Methods("GET")

	router.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Hi!"))
	}).Methods("GET")

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
