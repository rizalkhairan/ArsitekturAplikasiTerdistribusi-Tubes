# Sensor Pantai — discovery notes and quirks

The image is a black box:

```
sustuna/sensor-pantai@sha256:51fbd90d3891ad6e9f5768ad330ffb65205644cb29e50da65c7538f2e0b4397d
```

We never modify or replace it. We do not add replicas or a persistent volume for
`/data`. The client treats every quirk below generically; no code reads
`PANTAI_SEED`, `PANTAI_PROFILE`, `PANTAI_SPEED`, an observation-id prefix, or an
ordering assumption.

## How discovery is done (and its limitation)

`scripts/sensor-discovery.sh` runs the image locally with the practice profile
and probes it across time and seeds. It records:

- `/healthz` at each sample and whether it stays 200;
- `GET /v1/stations` and `GET /v1/observations` with/without cursors;
- `generated_at` vs the newest `observed_at` (staleness);
- the spacing of new observation ids (silence vs burst);
- malformed items and their JSON shape;
- behavior after `docker restart` (emptyDir-like state reset);
- different `PANTAI_SEED` values and both documented profiles (`practice`, `exam`).

> **Session limitation:** Docker Engine was not running in the development
> session where this code was written, and no Kubernetes context was available.
> The image was therefore **not executed here**. The table distinguishes
> *contract-documented* behavior from *discovery targets*. Run the script on the
> demo machine, append the observations, and do not infer behavior from a
> practice profile in application logic.

## Quirks and handling

| # | Observed / expected quirk | Detection | Handling (generic) |
|---|---|---|---|
| 1 | **Silent** for long stretches, then a burst | `seconds_since_last_fresh_observation` grows; a page suddenly contains many items | Progress-based health, not `/healthz`; bounded page size, pagination by cursor, bounded DLQ queue, critical path unaffected by normal-path storage speed |
| 2 | **Bursts everything at once** | Many items with the same `generated_at` / close `observed_at` | Paginate with `next_cursor`, process page-at-a-time, dedupe on composite key, checkpoint only after durable handoff |
| 3 | **Claims healthy while data is hours stale** | `/healthz` 200 but `generated_at - observed_at` is large and no new ids | Ignore `/healthz`; track time since a *new* composite id; trip the breaker on staleness; expose `stale:true` + `as_of` |
| 4 | **Garbage records** (missing fields, wrong types, NaN-like strings, absurd values, unknown quality, unknown station, future timestamps) | Generic validation (`schema`, `field_type`, `non_finite`, `range`, `timestamp`, `future_timestamp`, `unknown_station`, `quality`) | Never map to HazardEvent; park raw payload in the DLQ with the reason; continue with valid siblings |
| 5 | **State reset after pod restart** (`emptyDir /data`) | Cursor request returns 400/409/422; or ids repeat after restart | Detect cursor error, restart from the beginning, filter to `last_committed observed_at - overlap`, dedupe on `station + observed_at + observation_id` |
| 6 | **`observation_id` repeats or restarts** | Same id with a different station/time, or id sequence resets | Composite key, never raw id; storage upsert is idempotent; notifier dedupes by composite |
| 7 | **Long downtime** | Lease checkpoint `last_observed_at` is old; first page after recovery is large | Bounded pages, overlap window, checkpoint after storage/outbox durability; no assumption about how far back the sensor keeps data |
| 8 | **`next_cursor` empty at the end / cursor not stable across restarts** | Empty next cursor; cursor invalid after restart | Empty cursor ends the page loop; next poll re-reads from the start and the overlap filter suppresses old items |
| 9 | **Duplicate delivery of the same observation across pages** | Same composite appears twice | Idempotent storage upsert + outbox dedupe key; progress tracker marks only first new id fresh |
| 10 | **Slow or 429 responses** | Latency > 2 s; HTTP 429 | Short timeout (3 s), retry with exponential backoff + full jitter, retry budget (10 tokens, 0.2/s refill), breaker opens on failure rate/slow calls |

## What is deliberately *not* done

- No `/healthz` trust.
- No fixed sleep tuned to `PANTAI_SPEED`.
- No seed/profile parsing.
- No assumption that observation ids are unique forever.
- No extra volume or replica.
- No reading of the key file. The exam overlay only mounts it; the client still
  uses the bearer token from Secret `pantai-token` (key `token`), the same Secret
  the pod uses.

## Discovery script output to attach to the report

Run:

```bash
./scripts/sensor-discovery.sh practice 42
./scripts/sensor-discovery.sh practice 7
./scripts/sensor-discovery.sh exam 42     # requires the exam key at demo time
```

It writes `docs/sensor-discovery-<profile>-<seed>.log`. Append the interesting
lines (health vs freshness, burst sizes, malformed examples) to this file. Do not
replace the generic handling with profile-specific branches.
