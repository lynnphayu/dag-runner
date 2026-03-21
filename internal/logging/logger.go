package logging

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

type contextKey string

const (
	requestIDKey contextKey = "request_id"
)

func NewLogger(service string) *slog.Logger {
	level := parseLevel(os.Getenv("LOG_LEVEL"))

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	})

	logger := slog.New(handler)

	if strings.TrimSpace(service) != "" {
		logger = logger.With("service", service)
	}

	return logger
}

func parseLevel(raw string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func WithRequestID(ctx context.Context, requestID string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, requestIDKey, strings.TrimSpace(requestID))
}

func RequestIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}

	value, ok := ctx.Value(requestIDKey).(string)
	if !ok {
		return ""
	}

	return strings.TrimSpace(value)
}

func FromContext(ctx context.Context, logger *slog.Logger) *slog.Logger {
	if logger == nil {
		logger = slog.Default()
	}

	requestID := RequestIDFromContext(ctx)
	if requestID == "" {
		return logger
	}

	return logger.With("request_id", requestID)
}
