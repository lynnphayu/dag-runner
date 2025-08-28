# Builder
FROM golang:1.23-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .

# Build with optimized flags for smallest binary
RUN CGO_ENABLED=0 GOOS=linux go build \
  -a -installsuffix cgo \
  -ldflags="-w -s" \
  -o dag-runner ./cmd/runner_web

# Runtime
FROM gcr.io/distroless/static-debian12

COPY --from=builder /app/dag-runner /dag-runner
EXPOSE 8888
ENV PORT=8888

# Use non-root user (distroless default)
USER nonroot:nonroot

ENTRYPOINT ["/dag-runner"]
