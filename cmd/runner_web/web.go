package main

import (
	"log/slog"
	"net/http"
	"os"
	"path/filepath"

	"github.com/gorilla/mux"
	"github.com/joho/godotenv"
	http_service "github.com/lynnphayu/dag-runner/api/v1/http"
	"github.com/lynnphayu/dag-runner/internal/logging"
	"github.com/lynnphayu/dag-runner/internal/services/manager"
	"github.com/lynnphayu/dag-runner/internal/services/runner"
	"github.com/rs/cors"
)

func main() {
	logger := logging.NewLogger("runner-web")
	slog.SetDefault(logger)

	root, err := os.Getwd()
	if err != nil {
		logger.Error("failed to get working directory", "error", err)
		os.Exit(1)
	}
	envPath := filepath.Join(root, ".env")
	if err := godotenv.Load(envPath); err != nil {
		logger.Warn("failed to load environment file", "path", envPath, "error", err)
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8888"
	}

	postgresURI := os.Getenv("DATABASE_URL")
	if postgresURI == "" {
		logger.Error("missing required environment variable", "name", "DATABASE_URL")
		os.Exit(1)
	}
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		logger.Error("missing required environment variable", "name", "MONGO_URI")
		os.Exit(1)
	}

	runnerService := runner.NewRunnerService(postgresURI, mongoURI)
	managerService := manager.NewManagerService(mongoURI)

	router := mux.NewRouter()
	runnerHandler := http_service.NewRunnerHandler(runnerService, managerService)
	managerHandler := http_service.NewManagerHandler(managerService, runnerService, router)

	runnerHandler.RegisterRoutes(router)
	managerHandler.RegisterRoutes(router)

	if err := runnerService.RegisterAllPublishedFlowRoutes(router); err != nil {
		logger.Error("failed to pre-register published DAG routes", "error", err)
		os.Exit(1)
	}

	router.Use(http_service.RequestIDMiddleware(logger))
	router.Use(http_service.RecoveryMiddleware(logger))
	router.Use(http_service.AccessLogMiddleware(logger))

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

	logger.Info("starting HTTP server", "port", port)
	if err := http.ListenAndServe(":"+port, handler); err != nil {
		logger.Error("http server stopped", "error", err)
		os.Exit(1)
	}
}
