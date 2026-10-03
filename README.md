# Tugas Besar IF4031 Arsitektur Aplikasi Terdistribusi

Sistem koordinasi kebencana yang menjembatani BMKG, PVMBG, dan BNPB.

## Arsitektur

```
┌──────────┐         ┌──────────────────┐         ┌──────────┐
│   CLI    │ ──────► │  Client-Facing   │ ──────► │   Auth   │
│          │  HTTP   │  API :9080        │  HTTP   │  :8000   │
│          │ ◄────── │                  │ ◄────── │  SQLite  │
└──────────┘         └────────┬─────────┘         └──────────┘
                              │ HTTP read
                     ┌────────┴─────────┐
                     │   BNPB           │
                     │   Aggregator     │
                     │   :8085          │
                     └────────┬─────────┘
                              │
                    ┌─────────┴─────────┐
                    │                   │
             ┌──────┴──────┐    ┌───────┴──────┐
             │    BMKG     │    │    PVMBG     │
             │  gRPC :8081 │    │  REST :8082  │
             └─────────────┘    └──────────────┘
```

## Services

| Service | Port | Protocol | Description |
|---------|------|----------|-------------|
| BMKG | 8081 | gRPC | Seismic & tsunami data source (mock) |
| PVMBG | 8082 | REST | Volcanic activity data source (mock) |
| BNPB Aggregator | 8085 | HTTP | Polls sources, stores canonical data, serves read API |
| Auth Service | 8000 | HTTP | Custom JWT auth — login, refresh, user management |
| Client-Facing API | 9080 | HTTP | Serves hazard data + auth proxy with scope-based filtering |

---

## Authentication Flow

The CLI talks **only** to the Client-Facing API. The Client-Facing API proxies auth requests to the Auth Service.

```
CLI → POST /auth/login {username, password}
  Client-Facing API → Auth Service
  Returns: {access_token, refresh_token}

CLI → POST /auth/refresh {refresh_token}
  Client-Facing API → Auth Service
  Returns: {new_access_token, new_refresh_token}

CLI → GET /hazards (with access_token)
  Client-Facing API → BNPB Aggregator
  Returns: filtered hazard data (based on user's scope)
```

---

## Environment Variables

### Root `.env.example`

```env
# === BMKG ===
BMKG_API_KEY=bmkg-secret-key-change-me

# === PVMBG ===
PVMBG_TOKEN=pvmbg-secret-token-change-me
PVMBG_MIN_DELAY_MS=500
PVMBG_MAX_DELAY_MS=3000

# === BNPB ===
BNPB_API_PORT=8085

# === Auth Service ===
JWT_SECRET=bnpb-jwt-secret-2026
MEDIA_USER_PASSWORD=media-pass-2026
FIELD_USER_PASSWORD=field-pass-2026
INTERNAL_USER_PASSWORD=internal-pass-2026
```

### services/bmkg/.env

| Variable | Default | Description |
|----------|---------|-------------|
| `BMKG_API_KEY` | (required) | Secret key for gRPC auth via `X-BMKG-Key` header |

### services/pvmbg/.env

| Variable | Default | Description |
|----------|---------|-------------|
| `PVMBG_TOKEN` | (required) | Bearer token for REST auth |
| `PVMBG_MIN_DELAY_MS` | 500 | Minimum artificial delay (ms) |
| `PVMBG_MAX_DELAY_MS` | 3000 | Maximum artificial delay (ms) |

### services/bnpb/.env

| Variable | Default | Description |
|----------|---------|-------------|
| `DB_PATH` | `./data/bnpb.db` | SQLite database file path |
| `BMKG_GRPC_ADDR` | `bmkg:8081` | BMKG gRPC server address |
| `BMKG_API_KEY` | (required) | BMKG auth key |
| `PVMBG_BASE_URL` | `http://pvmbg:8082` | PVMBG REST base URL |
| `PVMBG_TOKEN` | (required) | PVMBG auth token |
| `UPSTREAM_TIMEOUT` | `5s` | Timeout for upstream calls |
| `POLL_INTERVAL` | `3s` | Polling interval (2-5s recommended) |
| `BNPB_API_PORT` | `8085` | HTTP read API port |
| `INTERNAL_API_KEY` | (required) | Shared secret for internal service-to-service auth |

### services/auth/.env (via docker-compose environment)

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8000` | HTTP server port |
| `DB_PATH` | `/app/data/auth.db` | SQLite database file path |
| `JWT_SECRET` | (required) | JWT signing secret (HS256) |
| `ACCESS_TOKEN_TTL_SECONDS` | `60` | Access token TTL in seconds |
| `REFRESH_TOKEN_TTL_HOURS` | `24` | Refresh token TTL in hours |
| `MEDIA_USER_PASSWORD` | (required) | Password for media-user |
| `FIELD_USER_PASSWORD` | (required) | Password for field-user |
| `INTERNAL_USER_PASSWORD` | (required) | Password for internal-user |

### services/client-api/.env

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8080` | HTTP server port |
| `BNPB_API_URL` | `http://bnpb:8085` | BNPB Aggregator base URL |
| `BNPB_INTERNAL_KEY` | (required) | Shared secret for BNPB read API auth |
| `AUTH_SERVICE_URL` | `http://auth:8000` | Auth Service base URL |
| `JWT_SECRET` | (required) | JWT validation secret (must match auth service) |
| `UPSTREAM_TIMEOUT` | `5s` | Timeout for BNPB API calls |
| `MAX_CONCURRENT_REQUESTS` | `100` | Rate limit: max concurrent requests |
| `TOKEN_TTL_SECONDS` | `60` | Access token TTL in seconds |
| `MAX_LIMIT` | `100` | Maximum value for `limit` query parameter |

---

## File Structure & What Each File Does

### Root

| File | Purpose |
|------|---------|
| `docker-compose.yml` | Orchestrates all 5 containers |
| `.env.example` | Template for all environment variables |
| `.gitignore` | Ignores `.env`, `*.db`, `target/`, `*.jar`, IDE files |

### services/bmkg/ (Seismic & Tsunami Data Source)

| File | Purpose |
|------|---------|
| `main.go` | gRPC server, event seeding (20 events), event generation (every 10s), auth interceptor with correlation ID |
| `database.go` | SQLite CRUD for seismic events and tsunami warnings |
| `schema.sql` | Database schema (seismic_events, tsunami_warnings tables) |
| `proto/bmkg.proto` | Protobuf service definition |
| `Dockerfile` | Multi-stage build, Alpine runtime |
| `.env` | `BMKG_API_KEY` |

### services/pvmbg/ (Volcanic Activity Data Source)

| File | Purpose |
|------|---------|
| `main.go` | REST server, report seeding (20 reports), report generation (every 10s), auth, admin controls, correlation ID middleware |
| `database.go` | SQLite CRUD for volcanic reports |
| `schema.sql` | Database schema (volcanic_reports table) |
| `Dockerfile` | Multi-stage build, Alpine runtime |
| `.env` | `PVMBG_TOKEN`, `PVMBG_MIN_DELAY_MS`, `PVMBG_MAX_DELAY_MS` |

### services/bnpb/ (Aggregator + Canonical Store)

| File | Purpose |
|------|---------|
| `main.go` | Entry point, starts HTTP read API + polling goroutines |
| `config.go` | Loads env vars |
| `internal/api/server.go` | HTTP read API with correlation ID, logging, internal auth |
| `internal/api/hazards.go` | Hazard read handlers |
| `internal/communication/bmkg.go` | gRPC client for BMKG with correlation ID + latency logging |
| `internal/communication/pvmbg.go` | HTTP client for PVMBG with correlation ID + latency logging |
| `internal/communication/clients.go` | Client factory |
| `internal/database/sqlite.go` | SQLite connection (WAL mode) |
| `internal/database/schema.sql` | Schema: volcano_references, hazard_events, poll_cursors |
| `internal/database/hazards.go` | Hazard CRUD, ListHazards, GetHazard queries |
| `internal/database/cursors.go` | Cursor-based incremental poll tracking |
| `internal/processing/aggregator.go` | Maps BMKG/PVMBG data to canonical HazardEvent |
| `internal/processing/bmkg_merge.go` | Merge logic for late-arriving tsunami warnings |
| `internal/middleware/correlation.go` | Correlation ID middleware |
| `internal/middleware/logging.go` | Structured JSON logging middleware |
| `Dockerfile` | Multi-stage build, Alpine runtime |
| `.env` | DB path, upstream addresses, API port, internal key |

### services/auth/ (Custom Auth Service)

| File | Purpose |
|------|---------|
| `main.go` | HTTP server, user seeding, JWT configuration, middleware chain |
| `internal/handlers/handlers.go` | Login, refresh, health handlers; JWT generation; refresh token rotation |
| `internal/database/database.go` | SQLite connection, user seeding (env-based passwords), password hashing |
| `internal/database/schema.sql` | Schema: users, refresh_tokens tables |
| `internal/middleware/middleware.go` | Correlation ID + structured logging middleware |
| `Dockerfile` | Multi-stage build, Alpine runtime |

### services/client-api/ (Client-Facing API + Auth Proxy)

| File | Purpose |
|------|---------|
| `main.go` | HTTP server, routing, middleware chain, auth proxy endpoints |
| `config.go` | Loads env vars, initializes auth |
| `internal/auth/validator.go` | JWT validation, 60s TTL enforcement |
| `internal/auth/proxy.go` | Proxies login/refresh to Auth Service |
| `internal/auth/scope_guard.go` | Rejects media-scope clients requesting raw fields (403) |
| `internal/filter/fields.go` | Scope-based field filtering (summary vs full) |
| `internal/bnpb/client.go` | HTTP client for BNPB Aggregator |
| `internal/middleware/correlation.go` | Correlation ID middleware |
| `internal/middleware/logging.go` | Structured JSON logging middleware |
| `internal/middleware/ratelimit.go` | Semaphore-based rate limiter (429) |
| `Dockerfile` | Multi-stage build, Alpine runtime |
| `.env` | API port, BNPB URL, Auth Service URL, JWT secret, rate limit, TTL |

### client/ (Java CLI)

| File | Purpose |
|------|---------|
| `pom.xml` | Maven build with Jackson dependency |
| `src/main/java/com/bnpb/cli/CliApp.java` | CLI app — talks to Client-Facing API only (login, refresh, autorefresh, whoami, tokens, hazards, hazard, logout) |
| `readme.md` | CLI documentation |
| `target/` | Build output (gitignored) |

---

## Seeding Summary

| Service | What Seeds | Where | When |
|---------|-----------|-------|------|
| BMKG | 20 seismic events | `services/bmkg/main.go` → `seedEvents()` | On startup |
| PVMBG | 20 volcanic reports | `services/pvmbg/main.go` → `seedReports()` | On startup |
| BNPB | 1 volcano reference (Merapi) | `services/bnpb/internal/database/schema.sql` | On DB init |
| BNPB | Canonical events | Polls BMKG/PVMBG every 3s | Continuous |
| Auth | 3 users (media, field, internal) | `services/auth/internal/database/database.go` → `SeedUsers()` | On startup |

---

## User Accounts

| Username | Password | Category | Scope | Access |
|----------|----------|----------|-------|--------|
| `media-user` | `media-pass-2026` | media | summary | hazard_id, source, hazard_type, severity, area_name, occurred_at |
| `field-user` | `field-pass-2026` | field | full | All fields including coordinates and attributes |
| `internal-user` | `internal-pass-2026` | internal | full | All fields including coordinates and attributes |

---

## Quick Start

```bash
# 1. Start all services
docker compose up --build

# 2. Wait ~15 seconds for all services to be ready

# 3. Build the CLI client
cd client && mvn package

# 4. Run the CLI
java -jar target/bnpb-cli-client-1.0.0.jar
```

## CLI Usage

```
BNPB> login media-user media-pass-2026
BNPB> hazards
BNPB> hazards ?source=BMKG&limit=5
BNPB> whoami
BNPB> tokens
BNPB> autorefresh off
BNPB> refresh
BNPB> logout
BNPB> exit
```

See [client/readme.md](client/readme.md) for full CLI documentation.

---

## API Endpoints

### Client-Facing API (:9080)

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | /health | None | Health check |
| POST | /auth/login | None | Proxy login to Auth Service (username + password) |
| POST | /auth/refresh | None | Proxy refresh to Auth Service (refresh_token) |
| GET | /hazards | Bearer | List hazard events |
| GET | /hazards/:id | Bearer | Get single hazard event |

### Auth Service (:8000)

| Method | Path | Description |
|--------|------|-------------|
| GET | /health | Health check |
| POST | /auth/login | Username/password → access + refresh token |
| POST | /auth/refresh | Refresh token → new access + refresh token |

### BNPB Aggregator (:8085)

| Method | Path | Description |
|--------|------|-------------|
| GET | /health | Health check |
| GET | /hazards | List all canonical events |
| GET | /hazards/:id | Get single event |

---

## Testing Scenarios

### 1. Media Scope (Summary Fields Only)
```
BNPB> login media-user media-pass-2026
BNPB> hazards
# Returns: hazard_id, source, hazard_type, severity, area_name, occurred_at
# Does NOT include: latitude, longitude, attributes
```

### 2. Field Scope (All Fields)
```
BNPB> login field-user field-pass-2026
BNPB> hazards
# Returns: all fields including latitude, longitude, attributes
```

### 3. Token Expiry (60s TTL) with Auto-Refresh ON
```
BNPB> login media-user media-pass-2026
# Wait 60 seconds
BNPB> hazards
# Auto-refreshes token, returns data
```

### 4. Token Expiry with Auto-Refresh OFF
```
BNPB> login media-user media-pass-2026
BNPB> autorefresh off
# Wait 60 seconds
BNPB> hazards
# Returns 401 Unauthorized
BNPB> refresh
BNPB> hazards
# Works again
```

### 5. Explicit Raw Field Rejection
```
BNPB> login media-user media-pass-2026
BNPB> hazards ?fields=latitude
# Returns 403 Forbidden
```

### 6. Rate Limiting
```
# Send >100 concurrent requests
# Returns 429 Too Many Requests
```

---

## Technology Stack

| Component | Technology |
|-----------|------------|
| Language | Go 1.26 |
| Auth | Custom JWT (HS256) with refresh token rotation |
| Database | SQLite (BMKG, PVMBG, BNPB, Auth) |
| Communication | gRPC (BMKG), REST/HTTP (PVMBG, BNPB, Auth, Client API) |
| CLI | Java 17 |
| Deployment | Docker + Docker Compose |
