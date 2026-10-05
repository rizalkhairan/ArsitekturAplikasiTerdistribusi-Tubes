# BNPB CLI Client

A Java command-line client for the BNPB Client-Facing API. Authenticate as a user, query hazard events, and manage your session.

The CLI talks **only** to the Client-Facing API (`http://localhost:9080`). It never talks to the Auth Service directly; login/refresh go through `POST /auth/login` and `POST /auth/refresh` on the Client-Facing API.

## Prerequisites

- Java 17 or higher
- Maven 3.6+
- BNPB system running (`docker compose up --build`)

## Build

```bash
cd client
mvn package
```

The JAR will be created at `target/bnpb-cli-client-1.0.0.jar`.

## Run

```bash
java -jar target/bnpb-cli-client-1.0.0.jar
```

## Commands

### Authentication

| Command | Description |
|---------|-------------|
| `login <username> <password>` | Authenticate and receive access token (60s TTL) |
| `refresh` | Refresh access token manually (no username/password needed) |
| `autorefresh [on\|off]` | Toggle auto-refresh (no arg = toggle current state) |
| `logout` | Clear session and logout |

### User & Token Info

| Command | Description |
|---------|-------------|
| `whoami` | Display current user info (decoded from JWT claims, no server call) |
| `tokens` | Display all received tokens with timestamps and types |

### Hazard Data

| Command | What the CLI sends |
|---------|--------------------|
| `hazards <limit>` | `GET /hazards?limit=<limit>` |
| `hazards <source> <limit>` | `GET /hazards?source=<source>&limit=<limit>` |
| `hazards <severity> <limit>` | `GET /hazards?severity=<severity>&limit=<limit>` |
| `hazard <id>` | `GET /hazards/<id>` |

> **Important:** the server requires `limit` between 1–100, and the CLI sends **only what you type** — it does **not** add a default `limit`. There is no bare `hazards` command: running it sends `GET /hazards` with no `limit` and the server returns `400 limit must be between 1 and 100`. Always include a limit, e.g. `hazards 5`.

Argument parsing rules in `hazardsCommand`:
- Numeric token → `limit`
- `BMKG` / `PVMBG` (case-insensitive) → `source`
- Any other word → `severity`

### Session

| Command | Description |
|---------|-------------|
| `clear` | Clear terminal screen |
| `help` | Show available commands |
| `exit` / `quit` | Exit CLI (also clears session via `logout`) |

## Command Formats

```
login <username> <password>
hazards <limit>
hazards <source> <limit>
hazards <severity> <limit>
hazard <id>
autorefresh
autorefresh on
autorefresh off
refresh
whoami
tokens
logout
```

Examples:
```
hazards 5
hazards BMKG 5
hazards PVMBG 10
hazards SIAGA 5
hazard 01a0ff7c-4c73-7475-9d2a-164f10f16336
```

## Quick Start

```
BNPB> login media-user media-pass-2026
BNPB> hazards 5
BNPB> hazards BMKG 5
BNPB> whoami
BNPB> tokens
BNPB> autorefresh off
BNPB> logout
BNPB> exit
```

## User Accounts

Users are stored in the Auth Service's SQLite database. Each user has a **category** that determines their access scope:

| Username | Password | Category | Scope | Access |
|----------|----------|----------|-------|--------|
| `media-user` | `media-pass-2026` | media | summary | hazard_id, source, hazard_type, severity, area_name, occurred_at |
| `field-user` | `field-pass-2026` | field | full | All fields including coordinates and attributes |
| `internal-user` | `internal-pass-2026` | internal | full | All fields including coordinates and attributes |

## Session Persistence

Session is stored at `~/.bnpb-cli-session.json`:
- Current access and refresh tokens (full values — useful for curl crosschecks)
- Username
- User info (from JWT claims)
- Auto-refresh setting
- Token history

Automatically saved after login/refresh and loaded on startup. `logout` and `exit` clear it.

## Token Lifecycle

1. **Login** — Receive access token (60s TTL) + refresh token (24h TTL); adds a `login` entry to token history.
2. **Use** — Access token is used for API requests.
3. **Expire** — After 60s, API returns 401.
4. **Auto-refresh (ON, default)** — `hazards` / `hazard` check expiry first and call `refresh()` automatically, adding a `refresh` entry to token history.
5. **Manual refresh (OFF)** — Use `refresh` command to get new token.
6. **Old refresh token rejected** — After refresh, the old refresh token is revoked (rotation).

**Important `tokens` behavior:** `tokens` only *displays* history. It does **not** trigger auto-refresh. So if you wait 60s and run only `tokens`, you will still see 1 entry. Run `hazards 5` (with auto-refresh ON) to trigger a refresh, then `tokens` will show 2 entries.

**No manual re-login needed.** The refresh token is stored and used automatically (or manually with `refresh`).

## Error Handling

| Error | Meaning | Solution |
|-------|---------|----------|
| 401 Unauthorized | Token expired or invalid, or not logged in | Use `refresh` or `login` again |
| 403 Forbidden | Insufficient scope | Login with appropriate user |
| 400 Bad Request | Invalid parameters (e.g. missing/out-of-range `limit`) | Check command syntax; `limit` must be 1–100 |
| 404 Not Found | Hazard ID does not exist | Copy a valid `hazard_id` from `hazards` output |
| 429 Too Many Requests | Rate limit exceeded | Wait and retry |

## Configuration

The CLI connects to:
- Client-Facing API: `http://localhost:9080`

Edit the `API_URL` constant in `CliApp.java` to change, then rebuild with `mvn package`.

---

## Manual Testing Guide

### 0. Setup

```bash
# From repo root
docker compose up --build

# Wait ~15 seconds until all 5 containers are Up:
docker compose ps

# In another terminal, build and run the CLI:
cd client
mvn package
java -jar target/bnpb-cli-client-1.0.0.jar
```

### 1. Smoke Test

```
BNPB> login media-user media-pass-2026
BNPB> hazards 5
```

**Expected:** `Login successful!`, then a JSON array of up to 5 hazard events. Confirms auth + Client-Facing API + BNPB + mocks all work.

### 2. Media Scope (Summary Fields Only)

```
BNPB> login media-user media-pass-2026
BNPB> hazards 5
```

**Expected fields per event:** `hazard_id`, `source`, `hazard_type`, `severity`, `area_name`, `occurred_at`.

**Must NOT be present:** `latitude`, `longitude`, `attributes`, `source_ref_id`, `ingested_at`.

### 3. Field Scope (All Fields)

```
BNPB> logout
BNPB> login field-user field-pass-2026
BNPB> hazards 5
```

**Expected:** All fields present, including `latitude`, `longitude`, `attributes`, `source_ref_id`, `ingested_at`.

### 4. Compare Access Levels

Run `hazards 5` as `media-user`, then `logout` and repeat as `field-user`.

**Expected:** media response has 6 fields per event; field response has 11 fields per event. Same underlying events, different projections.

### 5. Filtering

```
BNPB> hazards BMKG 5
BNPB> hazards PVMBG 5
BNPB> hazards SIAGA 5
BNPB> hazards BMKG SIAGA 5
```

**Expected:** only events matching the given source/severity; at most the requested limit. Note argument order does not matter (`hazards 5 BMKG` works the same as `hazards BMKG 5`).

### 6. Single Hazard Lookup

```
BNPB> login media-user media-pass-2026
BNPB> hazards 5
# copy a hazard_id from the output
BNPB> hazard <paste-id-here>
```

**Expected:** single JSON object for that hazard, filtered by your scope. Unknown ID returns `404`.

### 7. Token Expiry + Auto-Refresh ON (Default)

```
BNPB> login media-user media-pass-2026
BNPB> hazards 5
[Wait 60+ seconds]
BNPB> hazards 5
BNPB> tokens
```

**Expected:** second `hazards` prints `Token expired. Auto-refreshing...`, then returns data. `tokens` now shows 2 entries (`login` + `refresh`).

### 8. Token Expiry + Auto-Refresh OFF (Manual Refresh)

```
BNPB> login media-user media-pass-2026
BNPB> autorefresh off
[Wait 60+ seconds]
BNPB> hazards 5
[Returns 401 Unauthorized]
BNPB> refresh
BNPB> hazards 5
```

**Expected:** `hazards` fails with 401 while expired; `refresh` succeeds without username/password; `hazards` works again.

### 9. `tokens` Does Not Trigger Refresh

```
BNPB> login media-user media-pass-2026
[Wait 60+ seconds]
BNPB> tokens
```

**Expected:** still 1 entry. `tokens` never refreshes; only `hazards` / `hazard` trigger auto-refresh (and only when auto-refresh is ON).

### 10. `whoami` Shows JWT Claims

```
BNPB> login media-user media-pass-2026
BNPB> whoami
```

**Expected:** decoded JWT payload (`sub`, `name`, `scope`, `iat`, `exp`). No server call is made; shows locally stored claims.

### 11. Invalid Token / Not Logged In (401)

```
BNPB> logout
BNPB> hazards 5
```

**Expected:** `401 Unauthorized` (CLI prints "Not logged in" before even calling the server if no token is stored).

### 12. Non-Existent Hazard (404)

```
BNPB> login media-user media-pass-2026
BNPB> hazard non-existent-id
```

**Expected:** `404 Not Found`.

### 13. Input Validation (400)

```
BNPB> hazards 0
BNPB> hazards 101
```

**Expected:** `400 limit must be between 1 and 100` in both cases.

Note: there is intentionally no bare `hazards` command (it is not listed in `help`). It would send `GET /hazards` with no `limit` and return the same 400.

Note: `hazards abc` does **not** return 400 — `abc` is treated as a `severity` filter and returns `200` with a (likely empty) list.

### 14. Raw Field Rejection (403) — API Level, Not CLI

The CLI has **no syntax** to request raw fields (it never sends `?fields=`, `?include=`, or `X-Raw-Fields`). So `hazards latitude` does **not** return 403 — `latitude` is parsed as a severity filter.

To verify 403, use curl with a token from `~/.bnpb-cli-session.json`:

```bash
TOKEN=$(python3 -c "import json;print(json.load(open('$HOME/.bnpb-cli-session.json'))['accessToken'])")
curl -s "http://localhost:9080/hazards?fields=latitude" -H "Authorization: Bearer $TOKEN"
# Expected: 403 forbidden
```

### 15. Rate Limiting (429) — Load Test, Not Single CLI

A single CLI cannot easily trigger the 100-concurrent-request limit. Use the load generator in [`tools/loadtest`](../tools/loadtest/README.md). Single CLI requests should normally return 200.

### 16. Logout

```
BNPB> logout
BNPB> hazards 5
BNPB> tokens
```

**Expected:** `hazards` fails (not logged in); `tokens` shows "No tokens received yet" because logout clears history.

---

## Quick Verification Commands

```bash
# 1. Start system
docker compose up --build

# 2. Build CLI
cd client && mvn package

# 3. Run CLI
java -jar target/bnpb-cli-client-1.0.0.jar

# 4. Test flow
BNPB> login media-user media-pass-2026
BNPB> hazards BMKG 5
BNPB> whoami
BNPB> tokens
BNPB> logout
BNPB> exit
```
