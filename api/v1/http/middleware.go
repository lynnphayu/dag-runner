package http_endpoint

import (
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/lynnphayu/dag-runner/internal/logging"
)

const requestIDHeader = "X-Request-ID"

type muxMiddleware = mux.MiddlewareFunc

type responseRecorder struct {
	http.ResponseWriter
	statusCode int
	bytes      int
}

func newResponseRecorder(w http.ResponseWriter) *responseRecorder {
	return &responseRecorder{
		ResponseWriter: w,
		statusCode:     http.StatusOK,
	}
}

func (r *responseRecorder) WriteHeader(statusCode int) {
	r.statusCode = statusCode
	r.ResponseWriter.WriteHeader(statusCode)
}

func (r *responseRecorder) Write(data []byte) (int, error) {
	n, err := r.ResponseWriter.Write(data)
	r.bytes += n
	return n, err
}

func RequestIDMiddleware(logger *slog.Logger) muxMiddleware {
	return func(next http.Handler) http.Handler {
		baseLogger := logger
		if baseLogger == nil {
			baseLogger = slog.Default()
		}

		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestID := r.Header.Get(requestIDHeader)
			if requestID == "" {
				requestID = uuid.NewString()
			}

			ctx := logging.WithRequestID(r.Context(), requestID)
			w.Header().Set(requestIDHeader, requestID)

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func RecoveryMiddleware(logger *slog.Logger) muxMiddleware {
	return func(next http.Handler) http.Handler {
		baseLogger := logger
		if baseLogger == nil {
			baseLogger = slog.Default()
		}

		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if recovered := recover(); recovered != nil {
					ctx := r.Context()
					requestLogger := logging.FromContext(ctx, baseLogger)

					requestLogger.ErrorContext(
						ctx,
						"unhandled panic recovered",
						"panic", fmt.Sprintf("%v", recovered),
						"method", r.Method,
						"path", r.URL.Path,
						"remote_addr", r.RemoteAddr,
						"stack_trace", string(debug.Stack()),
					)

					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusInternalServerError)
					_, _ = w.Write([]byte(`{"error":"internal server error"}`))
				}
			}()

			next.ServeHTTP(w, r)
		})
	}
}

func AccessLogMiddleware(logger *slog.Logger) muxMiddleware {
	return func(next http.Handler) http.Handler {
		baseLogger := logger
		if baseLogger == nil {
			baseLogger = slog.Default()
		}

		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			startedAt := time.Now()
			recorder := newResponseRecorder(w)

			next.ServeHTTP(recorder, r)

			ctx := r.Context()
			requestLogger := logging.FromContext(ctx, baseLogger)

			level := slog.LevelInfo
			if recorder.statusCode >= http.StatusInternalServerError {
				level = slog.LevelError
			} else if recorder.statusCode >= http.StatusBadRequest {
				level = slog.LevelWarn
			}

			requestLogger.LogAttrs(
				ctx,
				level,
				"http request completed",
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.String("query", r.URL.RawQuery),
				slog.String("remote_addr", r.RemoteAddr),
				slog.String("user_agent", r.UserAgent()),
				slog.Int("status_code", recorder.statusCode),
				slog.Int("response_bytes", recorder.bytes),
				slog.Int64("duration_ms", time.Since(startedAt).Milliseconds()),
			)
		})
	}
}
