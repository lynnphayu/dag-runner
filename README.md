# DAG Runner

Execute data workflows defined as DAGs and expose them as HTTP endpoints.

This service lets you:

- Define workflows as a DAG of actions (query/insert/update/delete/join/filter/map/cond/http)
- Persist DAGs and HTTP adapters in MongoDB
- Execute actions against PostgreSQL and external HTTP services
- Optionally expose a DAG as an HTTP endpoint (with Basic, Bearer/JWKS or HMAC, and API Key auth)

---

This repository is part of a larger, multi-tenant system. You can run it standalone for local development and testing, or integrate it into the broader platform via its HTTP APIs to manage DAGs and register runtime endpoints. Feel free to adapt the code to your use case.

## Requirements

- Go 1.23+
- PostgreSQL (for data actions)
- MongoDB (stores DAG and adapter definitions)

## Configuration

Set the following environment variables:

```bash
export DATABASE_URL="postgres://user:password@host:5432/dbname?sslmode=disable"
export MONGO_URI="mongodb://user:password@host:27017/dag_manager"
export PORT=8888 # optional, defaults to 8888
```

## Run (local)

```bash
go run ./cmd/runner_web
```

Live reload (optional) with Air:

```bash
go install github.com/air-verse/air@latest
air -c .air.toml
```

## Run (Docker)

### Files Overview

- `Dockerfile` – Optimized multi-stage build using distroless (~10MB)
- `.dockerignore` – Excludes unnecessary files from Docker context

### Build the Docker Image

```bash
# Build the optimized image (~10MB)
docker build -t dag-runner .

# Build with a specific tag
docker build -t dag-runner:v1.0.0 .
```

### Run the Container

```bash
# Run with environment variables
docker run -p 8888:8888 \
  -e DATABASE_URL="postgres://user:password@host:5432/dbname?sslmode=disable" \
  -e MONGO_URI="mongodb://user:password@host:27017/dbname" \
  -e PORT="8888" \
  dag-runner

# Run in background (detached)
docker run -d -p 8888:8888 \
  --name dag-runner-app \
  -e DATABASE_URL="your_postgres_url" \
  -e MONGO_URI="your_mongo_uri" \
  dag-runner

# Run with environment file
docker run -p 8888:8888 --env-file .env dag-runner
```

### Environment Variables

The application requires these environment variables:

- `DATABASE_URL` – PostgreSQL connection string
- `MONGO_URI` – MongoDB connection string
- `PORT` – HTTP server port (default: 8888)

#### Example Environment File (.env)

```env
DATABASE_URL=postgres://user:password@localhost:5432/dag_runner?sslmode=disable
MONGO_URI=mongodb://user:password@localhost:27017/dag_manager
PORT=8888
```

---

## API

Base URL: `http://localhost:${PORT}` (default `8888`)

### Manager (DAG and Adapter CRUD)

- `POST /v1/dags` – Save a DAG
- `GET /v1/dags` – List DAGs
- `GET /v1/dags/{id}` – Get DAG by id
- `PUT /v1/dags/{id}` – Update DAG by id
- `DELETE /v1/dags/{id}` – Delete DAG by id
- `POST /v1/adapters` – Save an adapter (e.g., HTTP adapter)

### Runner (Introspection)

- `GET /v1/tables` – List Postgres table names
- `GET /v1/tables/{name}` – Get Postgres table columns schema
- `GET /v1/dags/{id}/register` – Register HTTP route for the DAG id

### Dynamic HTTP endpoints (from adapters)

Register at runtime by calling:

- `GET /v1/dags/{id}/register` – registers the HTTP route based on the stored HTTP adapter (uses `meta.path` and `meta.method`)

To expose your own endpoint(s):

1. Save a DAG (`POST /v1/dags`), get its `id`
2. Save an HTTP adapter (`POST /v1/adapters`) that references the DAG id in `graphId`
3. Call `GET /v1/dags/{id}/register` to register the HTTP route

---

## Data model

### DAG graph

Stored in MongoDB database `dag_manager`, collection `dags`:

- `Graph[*Action]` has `id` and `nodes` map
- Node `data.type` can be one of:
  - `query`, `insert`, `update`, `delete`
  - `join`, `filter`, `map`, `cond`
  - `http`

Example (excerpt) – see `example.json` for a complete sample:

```json
{
  "id": "7cbf3569-2cba-4e53-9c69-a9fa811be3b4",
  "nodes": {
    "n_users": {
      "id": "n_users",
      "name": "select_users",
      "data": {
        "type": "query",
        "meta": {
          "table": "users",
          "select": ["id", "name", "age"],
          "where": {}
        }
      },
      "dependencies": [],
      "dependents": ["n_join"]
    },
    "n_orders": {
      "id": "n_orders",
      "name": "select_orders",
      "data": {
        "type": "query",
        "meta": {
          "table": "orders",
          "select": ["id", "user_id", "total"],
          "where": {}
        }
      },
      "dependencies": [],
      "dependents": ["n_join"]
    },
    "n_join": {
      "id": "n_join",
      "name": "join_users_orders",
      "data": {
        "type": "join",
        "meta": {
          "on": { "id": "user_id" },
          "type": "inner",
          "left": "$results.n_users",
          "right": "$results.n_orders"
        }
      },
      "dependencies": ["n_users", "n_orders"],
      "dependents": []
    }
  }
}
```

### HTTP adapter

Stored in MongoDB database `dag_manager`, collection `adapters`:

```json
{
  "type": "http",
  "graphId": "7cbf3569-2cba-4e53-9c69-a9fa811be3b4",
  "input": {
    "limit": "$query.limit",
    "userAgent": "$headers['User-Agent']"
  },
  "meta": {
    "path": "/api/user-orders",
    "method": "POST",
    "response": "$results.n_join",
    "authType": "bearer",
    "auth": {
      "jwksUrl": "https://issuer.example.com/.well-known/jwks.json",
      "audience": "api",
      "issuer": "https://issuer.example.com"
    },
    "bodySchema": {},
    "querySchema": {}
  }
}
```

When a request hits `meta.path`, the service:

- Reads JSON body (optional) and query/path/headers
- Resolves the adapter `input` map into a DAG input context
- Executes the DAG
- Resolves `meta.response` against the execution results and returns it as JSON

---

## Auth options for HTTP adapters

- Basic

  ```json
  { "authType": "basic", "auth": { "username": "user", "password": "pass" } }
  ```

- Bearer (JWKS)

  ```json
  {
    "authType": "bearer",
    "auth": {
      "jwksUrl": "https://issuer/.well-known/jwks.json",
      "audience": "api",
      "issuer": "https://issuer"
    }
  }
  ```

  You can also embed JWKS inline via `auth.jwks` as an object or JSON string.

- API Key

  ```json
  {
    "authType": "apiKey",
    "auth": { "name": "X-API-Key", "in": "header", "value": "<key>" }
  }
  ```

Note: The manager validation currently requires `jwks` or `jwksUrl` for bearer auth when saving an adapter.

---

## Expressions and value resolution

- In adapter input mapping (before DAG execution), you can use: `$headers`, `$body`, `$query`, `$path` to map request data into the DAG `input`.
- Within DAG actions, use: `$input` (the mapped input) and `$results.<nodeId>` for referencing prior node outputs.
- Inline expressions: `${ ... }` are evaluated with the `expr` engine.

Examples:

```json
{
  "map": { "user_id": "$results.n_join[0].id" },
  "url": "https://api.example.com/users/${results.n_join[0].id}"
}
```

---

## Project layout (key parts)

- `cmd/runner_web` – HTTP server entrypoint
- `api/v1/http` – HTTP handlers (manager + runner)
- `internal/services/manager` – Mongo-backed CRUD for DAGs/adapters and validation
- `internal/services/runner` – Execution service, dynamic route registration, auth
- `internal/repositories/postgres` – Postgres repository + query builder
- `internal/repositories/mongodb` – Mongo repository
- `pkg/dag` – Core DAG engine (graph, executor, actions, value resolution)

---

## Notes & limitations

- HTTP routes are registered at runtime via `GET /v1/dags/{id}/register` based on the saved HTTP adapter for that DAG id.
- CORS is enabled and currently allows all origins. Restrict in production.
- Ensure your Postgres has the tables referenced by your DAG (e.g., `users`, `orders`, etc.).

---

## License

MIT
