# BNPB CLI Client

A Java command-line client for the BNPB Client-Facing API. Authenticate as a user, query hazard events, and manage your session.

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

## Authentication Flow

The CLI talks **only** to the Client-Facing API. The Client-Facing API proxies auth requests to Casdoor using the application credentials (stored server-side).

```
CLI → POST /auth/login {username, password}
  Client-Facing API → Casdoor (with app credentials)
  Returns: {access_token, refresh_token}

CLI → POST /auth/refresh {refresh_token}
  Client-Facing API → Casdoor (with app credentials)
  Returns: {new_access_token, new_refresh_token}
```

The CLI only needs **username** and **password**. No Casdoor application credentials needed.

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
| `whoami` | Display current user info (from JWT claims) |
| `tokens` | Display all received tokens with timestamps and types |

### Hazard Data

| Command | Description |
|---------|-------------|
| `hazards` | List all hazard events |
| `hazards ?source=BMKG` | Filter by source (BMKG or PVMBG) |
| `hazards ?severity=SIAGA` | Filter by severity (NORMAL, WASPADA, SIAGA, AWAS) |
| `hazards ?limit=10` | Limit results (1-100) |
| `hazards ?source=BMKG&severity=SIAGA&limit=5` | Combined filters |
| `hazard <id>` | Get single hazard event by ID |

### Session

| Command | Description |
|---------|-------------|
| `clear` | Clear terminal screen |
| `help` | Show available commands |
| `exit` / `quit` | Exit CLI |

## Auto-Refresh

When auto-refresh is **ON** (default), the CLI automatically refreshes the access token when it expires (60s TTL). This means you can keep using `hazards` and `hazard` commands without manually refreshing.

When auto-refresh is **OFF**, the CLI will return a 401 error when the token expires. You must manually run `refresh` to get a new token.

```
BNPB> autorefresh          # Toggle auto-refresh on/off
BNPB> autorefresh on       # Explicitly enable
BNPB> autorefresh off      # Explicitly disable
```

The auto-refresh setting is persisted in the session file.

## Quick Start

```
BNPB> login media-user media-pass-2026
BNPB> hazards
BNPB> hazards ?source=BMKG&limit=5
BNPB> whoami
BNPB> tokens
BNPB> autorefresh off
BNPB> logout
BNPB> exit
```

## User Accounts

Users are stored in Casdoor's user database. Each user has a **category** that determines their access scope:

| Username | Password | Category | Scope | Access |
|----------|----------|----------|-------|--------|
| `media-user` | `media-pass-2026` | media | summary | hazard_id, source, hazard_type, severity, area_name, occurred_at |
| `field-user` | `field-pass-2026` | field | full | All fields including coordinates and attributes |
| `internal-user` | `internal-pass-2026` | internal | full | All fields including coordinates and attributes |

## Session Persistence

Session is stored at `~/.bnpb-cli-session.json`:
- Current access and refresh tokens
- Username
- User info (from JWT claims)
- Auto-refresh setting
- Token history

Automatically saved after login/refresh and loaded on startup.

## Token Lifecycle

1. **Login** — Receive access token (60s TTL) + refresh token (24h TTL)
2. **Use** — Access token is used for API requests
3. **Expire** — After 60s, API returns 401
4. **Auto-refresh (ON)** — CLI automatically refreshes the token
5. **Manual refresh (OFF)** — Use `refresh` command to get new token
6. **Old token rejected** — After refresh, old access token is rejected

**No manual re-login needed.** The refresh token is stored and used automatically (or manually with `refresh`).

## Error Handling

| Error | Meaning | Solution |
|-------|---------|----------|
| 401 Unauthorized | Token expired or invalid | Use `refresh` or `login` again |
| 403 Forbidden | Insufficient scope | Login with appropriate user |
| 400 Bad Request | Invalid parameters | Check command syntax |
| 429 Too Many Requests | Rate limit exceeded | Wait and retry |

## Configuration

The CLI connects to:
- Client-Facing API: `http://localhost:8080`

Edit the constant in `CliApp.java` to change.
