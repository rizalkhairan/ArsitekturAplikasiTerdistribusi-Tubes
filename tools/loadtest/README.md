# Load test tool

Small, dependency-free Go load generator used for **Problem 2** (resilience and
controlled degradation). It logs in to the Client-Facing API, keeps a valid access
token (re-authenticating on `401`), runs a fixed number of concurrent workers for a
fixed duration, and reports throughput, p50/p95/p99 latency, error rate, and a
status-code histogram. HTTP `429` responses are counted separately as controlled
rejection (they are not treated as errors).

## Run against the Docker stack

```bash
# from the repo root; the compose network name is <project>_default
NET=$(docker network ls --format '{{.Name}}' | grep tubes | head -n1)

# 50 concurrent clients for 60 seconds against the full hazard list
docker run --rm --network "$NET" -v "$PWD/tools/loadtest:/src" -w /src \
  golang:1.26-alpine go run . \
  -base http://client-api:8080 -path '/hazards?limit=50' \
  -concurrency 50 -duration 60s -label sustained
```

## Flags

| Flag | Default | Meaning |
| --- | --- | --- |
| `-base` | `http://client-api:8080` | Base URL |
| `-path` | `/hazards?limit=100` | Request path (query included) |
| `-concurrency` | `50` | Parallel workers |
| `-duration` | `60s` | Test duration |
| `-user` / `-pass` | `field-user` / `field-pass-2026` | Login credentials |
| `-timeout` | `10s` | Per-request timeout |
| `-label` | `run` | Label printed with results |

## Head-of-line blocking check (Problem 2a)

Run two instances concurrently, one per source, while PVMBG is configured with a
3 s delay (`PVMBG_MIN_DELAY_MS=3000`, `PVMBG_MAX_DELAY_MS=3000`):

```bash
docker run ... go run . -path '/hazards?limit=100&source=BMKG' -label BMKG-only &
docker run ... go run . -path '/hazards?limit=100&source=PVMBG' -label PVMBG &
wait
```
