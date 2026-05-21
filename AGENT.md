# CLAUDE.md

Guidance for AI assistants (Claude, etc.) working in this repository.

> `CLAUDE.md` is a symlink to `AGENT.md` — edit `AGENT.md` and both names stay in sync.

## LLM Coding Guidelines

### Commenting Policy
- Do NOT add unnecessary comments.
- Avoid obvious comments that restate what the code already clearly expresses.
- Do NOT explain trivial operations.
- Prefer self-explanatory code through clear naming and structure instead of comments.

## Project Overview

**DAG Runner** is a Go service that executes data workflows defined as directed
acyclic graphs (DAGs) and exposes them as dynamic HTTP endpoints.

- DAGs are graphs of typed **actions** (`query`/`insert`/`update`/`delete`,
  `join`/`filter`/`map`/`cond`, `http`).
- Graph and adapter definitions are persisted in **MongoDB** (database `dag_manager`).
- Data actions run against **PostgreSQL**; `http` actions call external services.
- An **HTTP adapter** maps a request (headers/body/query/path) onto a DAG's input,
  runs the DAG, and shapes the response — registering a runtime route.

Module path: `github.com/lynnphayu/dag-runner`. Requires **Go 1.23+**.

## Commands

```bash
# Run the HTTP server (only functional entrypoint)
go run ./cmd/runner_web

# Build
go build -o dag-runner ./cmd/runner_web

# Live reload (install: go install github.com/air-verse/air@latest)
air -c .air.toml

# Compile-check everything / vet
go build ./...
go vet ./...

# Docker (multi-stage, distroless, ~10MB)
docker build -t dag-runner .
```

There is **no test suite** — no `*_test.go` files exist. Verify changes with
`go build ./...` / `go vet ./...` and, when possible, by running the server.

`cmd/runner_script` is a stub: its `main` is entirely commented out. Only
`cmd/runner_web` is wired up.

A Nix dev shell (`flake.nix`) provides Go + Postgres with `start-db` / `stop-db`
helper scripts (Postgres on port 5433).

## Configuration (environment variables)

| Var            | Required | Notes                                              |
|----------------|----------|----------------------------------------------------|
| `DATABASE_URL` | yes      | PostgreSQL connection string                       |
| `MONGO_URI`    | yes      | MongoDB connection string (db `dag_manager`)       |
| `PORT`         | no       | HTTP port, defaults to `8888`                      |
| `LOG_LEVEL`    | no       | `debug`/`info`/`warn`/`error`, defaults to `info`  |

`cmd/runner_web` loads a local `.env` via `godotenv` if present.

## Architecture

Layered, dependency flows downward; `pkg/dag` is the standalone engine with no
knowledge of HTTP or MongoDB.

```
cmd/runner_web            process entrypoint, wiring, CORS, middleware
  └─ api/v1/http          HTTP handlers, middleware, JSON response helpers
       ├─ internal/services/manager   DAG/adapter CRUD + versioning (Mongo-backed)
       └─ internal/services/runner    DAG execution, dynamic route registration, auth
            ├─ internal/repositories/postgres   pgx pool + SQL query builder
            ├─ internal/repositories/mongodb    Mongo driver wrapper
            └─ internal/repositories/http       outbound HTTP client
                 └─ pkg/dag           core DAG engine (graph, executor, actions)
                      └─ pkg/dag/validation   graph & adapter validation
```

Key directories:

- `cmd/runner_web/web.go` — builds services, registers routes, pre-registers all
  published DAG routes, applies CORS + middleware, starts the server.
- `api/v1/http/` — `handlers.go` (runner: tables, register), `manager_handlers.go`
  (DAG/adapter CRUD), `middleware.go` (request ID, panic recovery, access log),
  `respond.go` (JSON write helpers).
- `internal/services/manager/` — Mongo-backed CRUD with the versioning model.
- `internal/services/runner/` — loads published graphs, builds `dag.Runner`,
  implements auth, registers/replaces `mux` routes at runtime.
- `internal/constants/collections.go` — Mongo collection names: `graphs`,
  `nodes`, `adapters`.
- `internal/logging/` — structured `slog` JSON logger + request-ID context.
- `pkg/dag/` — engine: `node.go` (Graph/Node, topo sort, cycle detection),
  `action.go` (action types + registry), `executor.go` (`Runner`),
  `step_execution.go` (`ExecutionContext`, scheduling, condition eval),
  `execution_policy.go` (criticality policies), `adapter.go` (adapter types),
  `helpers.go` (join/filter/map + expression resolution).
- `pkg/utils/` — numeric/type helpers.

## DAG Data Model

A `Graph[*Action, any]` has metadata (`id`, `name`, `version`, `subversion`,
`status`, `userId`) plus a `nodes` map and `adapters` slice.

A `Node[*Action]` has `id`, `name`, `data` (the action), and `dependencies`
(node IDs it waits on). `dependents` is **not serialized** — it is recomputed
in memory from `dependencies` after load (`rebuildDependents`). Edges are
bidirectional and validated for consistency; cycles are rejected.

### Action types (`pkg/dag/action.go`)

`query`, `insert`, `update`, `delete`, `join`, `filter`, `map`, `cond`, `http`.

Each action implements `ActionInterface` (`Validate` + `Execute`) and is
registered in `actionRegistry`. The concrete struct is rebuilt at runtime by
JSON-marshalling `Action.Meta` and unmarshalling into the registered type.
**When adding an action type:** add the `ActionType` constant, the struct with
`Validate`/`Execute`, and register it in BOTH `actionRegistry`
(`pkg/dag/action.go`) and `actionTypeRegistry` (`pkg/dag/validation/validator.go`).

- `cond` is special: it evaluates a condition and rewrites its successors at
  runtime (`if` branch vs `else` branch) via `setSuccessors`.
- `join` requires exactly 2 dependencies; `filter`/`map` require exactly 1.

### HTTP Adapter (`pkg/dag/adapter.go`)

`Adapter[T]` has a typed `Meta` plus raw `MetaRaw` JSON; a custom `UnmarshalJSON`
dispatches on `type`. Adapter type constants are `http_adapter` and
`schedular_adapter` (note: the README/older docs say `"http"` — the wire value
is `"http_adapter"`; see `samples/create-http-adapter.json`).

`HttpAdapter` meta: `path`, `method`, `response` (output selector map),
`authType`, `auth`, `bodySchema`, `querySchema`.

## Execution Engine

`dag.Runner` executes a graph with goroutine-per-node parallelism:

- Root nodes start concurrently; each node blocks on its predecessors'
  completion channels before running, then schedules its successors.
- Results and errors are collected into a `Context` under mutexes.
- An `ExecutionPolicy` decides node **criticality**:
  - `StructuralExecutionPolicy` — every non-leaf node is critical
    (used by `Execute` / `ExecuteWithContext`).
  - `CriticalSetExecutionPolicy` — only nodes in a given set are critical
    (used by `ExecuteForResponse`, the HTTP-adapter path).
- A **critical** node failure cancels the whole run and returns an error.
  A **non-critical** failure is recorded in `ExecutionResult.Errors` and the run
  continues. `ExecuteForResponse` derives the critical set from the response
  selector's transitive dependency closure (`ExtractResponseDependencyClosure`).

### Expression resolution

Resolved by `ResolveValues` / `ResolveV2` (`helpers.go`) using the
`expr-lang/expr` engine, with `gjson`-based path access in the legacy `resolveV1`.

- Inside DAG actions: `$input` and `$results.<nodeId>` reference the input map
  and prior node outputs.
- Inside adapter `input` maps: `$headers`, `$body`, `$query`, `$path` reference
  the incoming request.
- `${ ... }` performs inline string interpolation; a bare leading `$` evaluates
  the whole expression.

## Versioning Model (`internal/services/manager`)

Graphs, nodes, and adapters live in **separate** Mongo collections, all keyed by
`graphId` + `version` + `subversion`. There is no in-place graph edit — every
change writes a new revision:

- `SaveDAG` — new graph at `version 1, subversion 1`, `status: draft`.
- `UpdateDAG` — if current is `draft`: bump **subversion**. If `published`:
  bump **version**, reset subversion to 1. Always writes new node/adapter rows.
- `PublishDAG` — flips the latest revision's `status` to `published` in place
  (no version bump); validates adapter response references first.
- Reads (`GetDAG`, `ListDAGs`, runner route loading) resolve the **latest**
  revision via `FindLatestByVersion` / aggregation sorting by
  `version desc, subversion desc`.

`X-User-Id` request header populates `userId` for multi-tenant scoping.

## HTTP API

Base URL `http://localhost:${PORT}` (default `8888`).

### Manager — DAG CRUD & versioning
- `POST   /v1/dags` — save a new DAG (returns 201)
- `GET    /v1/dags` — list latest DAGs (scoped by `X-User-Id`)
- `GET    /v1/dags/{id}` — get latest DAG by id
- `PUT    /v1/dags/{id}` — update (new draft subversion / published version)
- `DELETE /v1/dags/{id}` — delete DAG + its nodes + adapters (all versions)
- `POST   /v1/dags/{id}/publish` — publish; also registers the runtime route
- `GET    /v1/dags/{id}/versions` — list all versions/subversions

### Manager — Adapters
- `POST /v1/adapters` — save an adapter
- `GET  /v1/adapters` — list adapters (filter by `?graphId=` and `X-User-Id`)
- `GET  /v1/adapters/{id}` — get adapter by id

### Runner
- `GET  /v1/tables` — list Postgres public table names
- `GET  /v1/tables/{name}` — get a table's column → type schema
- `POST /v1/dags/{id}/register` — (re)register the runtime HTTP route for a DAG
  *(note: the README says `GET` — the implemented method is `POST`)*

### Misc
- `GET /health` → `OK`
- `GET /` → `Hi!`

Dynamic adapter routes are registered onto the shared `mux.Router` at runtime —
on publish, on explicit register, and at startup for every published graph
(`RegisterAllPublishedFlowRoutes`). Re-registering a graph whose path changed
neuters the old route.

## Auth (HTTP adapters)

`authType` ∈ `none` | `basic` | `bearer` | `apiKey`.

- `basic` — `auth.username` / `auth.password`.
- `bearer` — HMAC (`auth.secret`/`hmacSecret`/`sharedSecret`, optional `alg`
  HS256/384/512) **or** JWKS (`auth.jwks` inline object/string, or `auth.jwksUrl`).
  Optional `audience` / `issuer` claim checks.
- `apiKey` — `auth.name` + `auth.in` (`header`/`query`/`cookie`) + `auth.value`.

Auth is enforced in `runner.RegisterFlowRoute`; validation rules live in
`pkg/dag/validation/validator.go`.

## Conventions & Gotchas

- **Logging:** structured `slog` JSON only. Use `logging.FromContext(ctx, logger)`
  in handlers to attach the request ID. Do not use the `log` package.
- **Errors:** wrap with `fmt.Errorf("...: %w", err)`. HTTP handlers respond via
  the `write*` helpers in `respond.go` (`writeOK`, `writeCreated`,
  `writeInternalError`, `writeInvalidBodyError`, `writeNoContent`).
- **Repositories** share package names `respositories` (sic — note the typo) and
  `repositories`; keep the existing name when editing a file.
- **CORS** currently allows all origins (`*`) — intended to be restricted in
  production.
- **`dependents`** must never be serialized; it is always recomputed from
  `dependencies` after a graph is loaded.
- The README is the user-facing doc and is slightly out of date (adapter type
  string, the `register` HTTP verb). Prefer the code as the source of truth.
- CI (`.github/workflows/on-push.yml`) builds and pushes a multi-arch image to
  AWS ECR on push to `main`.

## Reference Samples

- `example.json` — full DAG illustrating query → join → http/insert fan-out.
- `samples/create-graph.json` — DAG payload for `POST /v1/dags`.
- `samples/create-http-adapter.json` — adapter payload for `POST /v1/adapters`.
</content>
</invoke>
