# syntax=docker/dockerfile:1.6
# Builder
FROM --platform=$BUILDPLATFORM golang:1.23-alpine AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /app

# Leverage module cache
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
  go mod download

COPY . .

# Build with cache for compiled packages; cross-compile per target
RUN --mount=type=cache,target=/root/.cache/go-build \
  CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
  go build -trimpath -ldflags="-w -s" -o dag-runner ./cmd/runner_web

# Runtime
FROM gcr.io/distroless/static-debian12

COPY --from=builder /app/dag-runner /dag-runner
EXPOSE 8888
ENV PORT=8888

# Use non-root user (distroless default)
USER nonroot:nonroot

ENTRYPOINT ["/dag-runner"]
