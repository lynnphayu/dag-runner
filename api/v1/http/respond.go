package http_endpoint

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/lynnphayu/dag-runner/internal/logging"
)

func writeJSON(w http.ResponseWriter, statusCode int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	if payload == nil {
		return
	}

	if err := json.NewEncoder(w).Encode(payload); err != nil {
		slog.Default().Error(
			"failed to encode json response",
			"status_code", statusCode,
			"error", err,
		)
	}
}

func writeJSONError(w http.ResponseWriter, r *http.Request, logger *slog.Logger, statusCode int, message string, err error) {
	requestLogger := logger
	if requestLogger == nil {
		requestLogger = slog.Default()
	}
	if r != nil {
		requestLogger = logging.FromContext(r.Context(), requestLogger)
	}

	attrs := []any{
		"status_code", statusCode,
		"message", message,
	}

	if r != nil {
		attrs = append(
			attrs,
			"method", r.Method,
			"path", r.URL.Path,
		)
	}

	if err != nil {
		attrs = append(attrs, "error", err)
	}

	switch {
	case statusCode >= http.StatusInternalServerError:
		requestLogger.Error("request failed", attrs...)
	case statusCode >= http.StatusBadRequest:
		requestLogger.Warn("request failed", attrs...)
	default:
		requestLogger.Info("request failed", attrs...)
	}

	writeJSON(w, statusCode, map[string]string{
		"error": message,
	})
}

func writeInvalidBodyError(w http.ResponseWriter, r *http.Request, logger *slog.Logger, err error) {
	writeJSONError(w, r, logger, http.StatusBadRequest, "invalid request body", err)
}

func writeInternalError(w http.ResponseWriter, r *http.Request, logger *slog.Logger, err error) {
	message := "internal server error"
	if err == nil {
		err = errors.New(message)
	}

	writeJSONError(w, r, logger, http.StatusInternalServerError, message, err)
}

func writeCreated(w http.ResponseWriter, payload interface{}) {
	writeJSON(w, http.StatusCreated, payload)
}

func writeOK(w http.ResponseWriter, payload interface{}) {
	writeJSON(w, http.StatusOK, payload)
}

func writeNoContent(w http.ResponseWriter) {
	w.WriteHeader(http.StatusNoContent)
}
