# Tugas Besar IF4031 Arsitektur Aplikasi Terdistribusi

Sistem koordinasi kebencana yang menjembatani BMKG, PVMBG, dan BNPB.

## Arsitektur

```
┌──────────────┐
│  PostgreSQL  │  (Casdoor's DB)
│  :5432       │
└──────┬───────┘
       │
┌──────┴───────┐         ┌──────────────────┐
│   Casdoor    │         │                  │
│  Auth/Token  │◄────────┤   Client-Facing  │
│  Service     │  app    │   API :8080      │
│  :8000       │  creds  │                  │
│              │         │  (Go)            │
└──────────────┘         └────────┬─────────┘
                                  │ HTTP read
                         ┌────────┴─────────┐
                         │   BNPB           │
                         │   Aggregator     │
                         │   + Store        │
                         │   :8085          │
                         └────────┬─────────┘
                                  │
                    ┌─────────────┴─────────────┐
                    │                           │
             ┌──────┴──────┐            ┌───────┴──────┐
             │    BMKG     │            │    PVMBG     │
             │  gRPC :8081 │            │  REST :8082  │
             └─────────────┘            └──────────────┘

  ┌──────────┐
  │   CLI    │  (Java) — talks to Client-Facing API only
  └──────────┘
```

## Services

| Service | Port | Protocol | Description |
|---------|------|----------|-------------|
| BMKG | 8081 | gRPC | Seismic & tsunami data source (mock) |
| PVMBG | 8082 | REST | Volcanic activity data source (mock) |
| BNPB Aggregator | 8085 | HTTP | Polls sources, stores canonical data, serves read API |
| Casdoor | 8000 | HTTP | OAuth2/OIDC auth server |
| PostgreSQL | 5432 | — | Casdoor's database |
| Client-Facing API | 8080 | HTTP | Serves hazard data + auth proxy with scope-based filtering |

---

## Authentication Flow

The CLI talks **only** to the Client-Facing API. The Client-Facing API proxies auth requests to Casdoor using the application credentials (stored server-side).

```
CLI → POST /auth/login {username, password}
  Client-Facing API → Casdoor (with app credentials)
  Returns: {access_token, refresh_token}

CLI → POST /auth/refresh {refresh_token}
  Client-Facing API → Casdoor (with app credentials)
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

# === Casdoor ===
CASDOOR_DB_PASSWORD=casdoor-db-password-change-me
CASDOOR_ADMIN_USER=admin
CASDOOR_ADMIN_PASSWORD=123
CASDOOR_ORG_NAME=bnpb
CASDOOR_APP_NAME=bnpb-client-api
SEED_USERS=true
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

### services/client-api/.env

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8080` | HTTP server port |
| `BNPB_API_URL` | `http://bnpb:8085` | BNPB Aggregator base URL |
| `BNPB_INTERNAL_KEY` | (required) | Shared secret for BNPB read API auth |
| `CASDOOR_ENDPOINT` | `http://casdoor:8000` | Casdoor server URL |
| `CASDOOR_CLIENT_ID` | (required) | Casdoor application client ID (from bootstrap) |
| `CASDOOR_CLIENT_SECRET` | (required) | Casdoor application client secret (from bootstrap) |
| `CASDOOR_CERTIFICATE` | (required) | Casdoor application certificate (PEM, from bootstrap) |
| `CASDOOR_ORG_NAME` | `bnpb` | Casdoor organization name |
| `CASDOOR_APP_NAME` | `bnpb-client-api` | Casdoor application name |
| `UPSTREAM_TIMEOUT` | `5s` | Timeout for BNPB API calls |
| `MAX_CONCURRENT_REQUESTS` | `100` | Rate limit: max concurrent requests |
| `TOKEN_TTL_SECONDS` | `60` | Access token TTL in seconds |
| `MAX_LIMIT` | `100` | Maximum value for `limit` query parameter |

### casdoor-bootstrap environment (from docker-compose.yml)

| Variable | Default | Description |
|----------|---------|-------------|
| `CASDOOR_ENDPOINT` | `http://casdoor:8000` | Casdoor server URL |
| `CASDOOR_ADMIN_USER` | `admin` | Casdoor admin username |
| `CASDOOR_ADMIN_PASSWORD` | `123` | Casdoor admin password |
| `CASDOOR_ORG_NAME` | `bnpb` | Organization name to create |
| `CASDOOR_APP_NAME` | `bnpb-client-api` | Application name to create |
| `SEED_USERS` | `true` | Whether to seed users (`true`/`false`) |

---

## File Structure & What Each File Does

### Root

| File | Purpose |
|------|---------|
| `docker-compose.yml` | Orchestrates all 7 containers |
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

### services/casdoor-bootstrap/ (Auth Bootstrap)

| File | Purpose |
|------|---------|
| `main.go` | Logs into Casdoor as admin, creates organization, application, 3 users with categories (if SEED_USERS=true), verifies users |
| `Dockerfile` | Multi-stage build, Alpine runtime |

### services/client-api/ (Client-Facing API + Auth Proxy)

| File | Purpose |
|------|---------|
| `main.go` | HTTP server, routing, middleware chain, auth proxy endpoints |
| `config.go` | Loads env vars, initializes Casdoor SDK |
| `internal/auth/validator.go` | JWT validation via Casdoor SDK, 60s TTL enforcement |
| `internal/auth/proxy.go` | **New** — Proxies login/refresh to Casdoor with app credentials |
| `internal/auth/scope_guard.go` | Rejects media-scope clients requesting raw fields (403) |
| `internal/filter/fields.go` | Scope-based field filtering (summary vs full) |
| `internal/bnpb/client.go` | HTTP client for BNPB Aggregator |
| `internal/middleware/correlation.go` | Correlation ID middleware |
| `internal/middleware/logging.go` | Structured JSON logging middleware |
| `internal/middleware/ratelimit.go` | Semaphore-based rate limiter (429) |
| `Dockerfile` | Multi-stage build, Alpine runtime |
| `.env` | API port, BNPB URL, Casdoor SDK config, rate limit, TTL |

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
| Casdoor | Org, app, 3 users | `services/casdoor-bootstrap/main.go` | On container start (if SEED_USERS=true) |

---

## User Accounts (from Casdoor Bootstrap)

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

# 2. Wait ~30 seconds for Casdoor to start and bootstrap to complete

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

### Client-Facing API (:8080)

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | /health | None | Health check |
| POST | /auth/login | None | Proxy login to Casdoor (username + password) |
| POST | /auth/refresh | None | Proxy refresh to Casdoor (refresh_token) |
| GET | /hazards | Bearer | List hazard events |
| GET | /hazards/:id | Bearer | Get single hazard event |

### BNPB Aggregator (:8085)

| Method | Path | Description |
|--------|------|-------------|
| GET | /health | Health check |
| GET | /hazards | List all canonical events |
| GET | /hazards/:id | Get single event |

### Casdoor (:8000)

| Method | Path | Description |
|--------|------|-------------|
| POST | /api/login | Admin login |
| POST | /api/login/oauth/access_token | OAuth2 token endpoint |
| POST | /api/login/oauth/refresh_token | OAuth2 refresh endpoint |
| GET | /api/userinfo | Get user info |

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
| Auth | Casdoor (OAuth2/OIDC) |
| Database | SQLite (BMKG, PVMBG, BNPB), PostgreSQL (Casdoor) |
| Communication | gRPC (BMKG), REST/HTTP (PVMBG, BNPB, Client API) |
| CLI | Java 17 |
| Deployment | Docker + Docker Compose |
