# BMKG mock service

BMKG exposes gRPC on port `8081` and stores mock data in SQLite. It seeds
20 historical seismic events (five with tsunami warnings) on a fresh database,
then adds an event every 10 seconds. Events with `potential_tsunami = true` get
a warning linked by `related_event_id`. Existing data survives restarts.

The implementation uses simple mock rules: magnitudes range from 4 to 8,
magnitudes of 7 or above produce warnings, and estimated arrival is 30 minutes
after the event. These values are for testing.

## Run

Create `services/bmkg/.env` from `.env.example` and set `BMKG_API_KEY`, then run
from the repository root:

```sh
docker compose up --build bmkg
```

The default database path is `/app/data/bmkg.db`, backed by the `bmkg-data`
Docker volume. For local Go execution, set `DB_PATH` to a writable file path
and export `BMKG_API_KEY`; the service does not load `.env` itself.

## Query

Reflection is not enabled, so pass the proto to `grpcurl`. Health needs no key:

```sh
grpcurl -plaintext -import-path services/bmkg/proto -proto bmkg.proto \
  localhost:8081 bmkg.BMKG/Health
```

Data RPCs require the `x-bmkg-key` metadata header and have a 50–150 ms delay:

```sh
grpcurl -plaintext -import-path services/bmkg/proto -proto bmkg.proto \
  -H 'x-bmkg-key: change-me' -d '{}' \
  localhost:8081 bmkg.BMKG/GetSeismicEvents

grpcurl -plaintext -import-path services/bmkg/proto -proto bmkg.proto \
  -H 'x-bmkg-key: change-me' -d '{"since":"2026-01-01T00:00:00Z"}' \
  localhost:8081 bmkg.BMKG/GetTsunamiWarnings
```

Omit `since` to retrieve all records. It is an inclusive UTC RFC3339 timestamp
filter on `occurred_at` for events and `estimated_arrival` for warnings.
Responses are ordered by those respective timestamps.

## Regenerate protobuf bindings

Generated Go files are included, so normal builds do not need `protoc`.
After changing the proto, install the generators and run from this directory
(with `protoc` installed and Go's bin directory on `PATH`):

```sh
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.1
protoc --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/bmkg.proto
```
