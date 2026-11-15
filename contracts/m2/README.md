# M2 canonical API contract — v1

This is the A02 integration baseline, derived from the root M2 specification
(contract changes and Appendix A) and `services/ARCHITECTURE.md`. Proposed
application decisions below need B/C review before production integration.
The supplied Audit contract takes precedence over application conventions.

## Run the integration stub

```bash
AUDIT_TOKEN=local-audit WRITE_TOKEN=local-write python3 tools/contract-stub/server.py
curl -H 'Authorization: Bearer local-audit' http://localhost:8095/internal/audit/events
curl -X PUT -H 'Authorization: Bearer local-write' -H 'Content-Type: application/json' \
  --data-binary @contracts/m2/write-example.json \
  http://localhost:8095/internal/canonical/events/SATELIT/fire-example
```

Python 3.10+, standard library only. Bind defaults to `127.0.0.1:8095`;
`--host`, `--port`, and `--fixtures` override defaults. Both tokens must be set
and distinct. Restart resets all writes and cursors. The stub has no persistent
storage, replication, database failover or broker; it must never acknowledge
real field reports or serve as a deployed canonical store. Its write responses
declare `durability: simulated`. Fixtures and locations are synthetic.

Run contract checks with `python3 -m unittest discover -s tools/contract-stub -p 'test_*.py'`.
Simulate incomplete reads with `--unavailable-unit shard-B`: all query responses
identify unavailable units and exclude their records. Remove that flag and restart
to restore the complete fixture dataset.

## Ownership and routing

Canonical storage remains inside the Aggregator (`services/bnpb`); PostgreSQL
is private to that service. A owns the storage/query/audit modules; B owns the
Aggregator server, authentication, routing, polling and lease authority; C owns
pipeline input, DLQ, offsets and processing state. C calls the authenticated
Aggregator write API and never connects to canonical tables. These are module
boundaries, not a new independently deployed canonical service.

| Route | Implementation owner | Caller / credentials |
| --- | --- | --- |
| `PUT /internal/canonical/events/{source}/{source_ref_id}` | A, mounted by B | B/C service identities, write scope |
| `GET /internal/canonical/events` | A, mounted by B | Internal read scope |
| `GET /internal/audit/events` | A, mounted by B | Assistant audit Bearer token |
| `GET /internal/audit/checksum` | A, mounted by B | Assistant audit Bearer token |
| `GET /internal/audit/location` | A, mounted by B | Assistant audit Bearer token |
| `GET /internal/audit/unprocessed` | C, proxied by B | Assistant audit Bearer token |
| `POST /field-reports` | C ingest, mounted by B/client API | Authenticated field-team write scope |

Keep existing M1 `/hazards`, scope filtering and authentication compatible.
Audit reads expose full fields; never route Media users through Audit. Production
credentials are per service and Secrets. The stub uses two local tokens only and
returns 503 for C-owned routes, so it cannot claim C's acceptance or DLQ state.

## Canonical event

Fields: `hazard_id`, `source`, `source_ref_id`, `hazard_type`, `severity`,
`area_name`, `latitude`, `longitude`, `occurred_at`, `ingested_at`, `attributes`.
See `fixtures.json`. Sources: `BMKG`, `PVMBG`, `SATELIT`, `SENSOR_PANTAI`.
Types: `SEISMIC`, `VOLCANIC`, `WILDFIRE`, `COASTAL`; tsunami is represented
through BMKG attributes/warnings as in M1, with explicit tsunami analytics
classification rather than silently inventing a new enum. Severity:
`NORMAL`, `WASPADA`, `SIAGA`, `AWAS`. Preserve additive fields inside the
`attributes` object. Times are ISO 8601 UTC. Coordinates are numbers within
[-90,90] and [-180,180]. Detection keys are verbatim feed strings, without rounding.

Identity is the exact UTF-8 pair `(source, source_ref_id)`. Do not concatenate
without unambiguous encoding or trim/case-fold IDs. A assigns a stable hazard ID
and first-ingestion timestamp; updates retain both. The stub uses SHA-256 of the
JSON-encoded pair as its deterministic ID; production must preserve existing M1
IDs on migration. The distribution key uses the encoded pair, never geography.

## Writes, versions and retractions

The PUT path carries identity; percent-encode each component. IDs containing `/`
are outside this v1 path contract. The body is the envelope in `write-example.json`:

- `version`: positive monotonically increasing integer per identity, assigned
  by its authoritative producer and persisted across restarts/replay.
- `operation`: `upsert` or `retract`. Upsert requires `event` with type, severity,
  area, coordinates, occurred_at and attributes. Retract omits `event` and keeps
  the identity's tombstone/version. Never recycle an ID for a different event.
- `authority`: `{partition, epoch}`. This is an assertion checked against trusted
  lease state, not proof of ownership. Production derives producer and allowed
  partition from service credentials. Partition is a feed or processing partition.
- `correlation_id`: nonempty identifier forwarded to logs/traces/outbox.

201 on first mutation; 200 on update, retract, or identical retry. Response:
`{hazard_id, version, operation, replayed, durability}`. A higher version wins;
lower version returns 409 `stale_version`; reuse of a version with different
operation/event returns 409 `version_conflict`. Retry equivalence excludes
correlation ID and authority so a new legitimate lease owner can retry the same
mutation. Authority must still validate on every retry. Retraction of an absent
identity creates a tombstone, preventing a delayed old upsert from reviving it.
A higher-version upsert can explicitly reactivate it. Active query/audit results
exclude tombstones. Retraction emits a downstream change; it never erases the
deduplication history. Clustering merges/splits must publish deterministic
versioned upserts/retractions for every affected identity.

Production transaction: lock trusted authority and event version; reject stale
owner/version; mutate projection/tombstone; append unique outbox change keyed by
`(source, source_ref_id, version)`; commit under replication policy before success.
Broker publication is at least once; consumers deduplicate this change key.
Do not use M1's initial-insert-only outbox constraint for updates/retractions.

## Authority and poll-window commit protocol (joint A/B handoff)

B's durable coordination store issues increasing epochs per partition using its
own authoritative time/CAS, never the poller's local clock. B authenticates lease
installation to A; A durably records `(partition, epoch, owner)` before permitting
that owner's writes. Each mutation serializes against that record. Installing a
higher epoch fences earlier epochs even after freeze/resume. A header alone, a
cached lease check, or a remote check before an independent commit is insufficient.

There is no implicit cross-database transaction between B's store and canonical
data. Proposed protocol: B leases a durable poll-window ID and expected cursor;
each result has deterministic identity/version and window ID; A durably accepts
the complete batch plus a sealed window receipt under the current epoch. B CAS
commits its cursor only after verifying that receipt, current ownership and expected
previous cursor. If B crashes before CAS, replay the same window/mutations and
reuse the receipt. If ownership changes, an old B cannot seal or commit a window;
the successor replays and seals under its own epoch. Zero-result windows also
need a receipt. B must not skip windows following a failed CAS. Batch/seal and
lease-install routes and cross-store reconciliation are deliberately **not frozen
by this stub**: A/B must settle those before A07; per-event PUT alone does not
establish exactly-once acquisition.

The stub has one fixed trusted partition per source (`BMKG`, `PVMBG`, `SATELIT`,
`SENSOR_PANTAI`), all at epoch 1. It rejects other partitions/epochs and only
simulates serialized writes. It has no lease issuance or lease-expiry claim.

## Queries and Audit (Appendix A)

`GET /internal/canonical/events` uses the same response and filters as audit events.
`source` optional, `since` inclusive on **ingested_at**, `limit` defaults to 100,
range 1–1000, `cursor` opaque. Sort ascending by `(ingested_at, hazard_id)`.
Response: `{items, next_cursor, partial, unavailable_units}`; final cursor is null.

Pagination decision: capture an immutable query snapshot on page one. Bind the
cursor to source/since and coverage. Subsequent pages repeat source/since; limit
may change. Snapshot TTL is 300 s; expired cursor returns 400 `cursor_expired`,
requiring a fresh traversal. Never silently restart. Cursors are process-local in
the stub; production must survive replica routing or explicitly reject unavailable
snapshots. Updates during traversal appear on a fresh traversal. Partial snapshots
retain their initial coverage; recovery requires starting again for complete data.

`GET /internal/audit/checksum?source=...`: source optional; respond
`{source, count, checksum, algorithm:"sha256", as_of, partial, unavailable_units}`.
With no source, `source` is null and all active records participate. Sort all
source_ref_id values by UTF-8 bytes (retain equal IDs from different sources),
join with `\n` without trailing newline, hash SHA-256 lowercase. Empty input is
SHA-256 of empty bytes. `as_of` identifies this independent read, not an events
cursor snapshot. Compare audit totals while ingestion is quiescent or using a
future explicitly coordinated snapshot.

`GET /internal/audit/location?hazard_id=...`: required ID; respond
`{hazard_id, locations:[{node,unit,role}], partial}`; role primary/replica,
node is the Kubernetes node name. During incomplete coverage also include
`unavailable_units`. Unknown ID is 404 only when coverage is complete; otherwise
503 `coverage_unavailable` avoids claiming absence. Production reports actual
placements, not the fixture topology. All reads with known surviving results
return 200 with partial markers when units are missing, never fabricated totals.

C's `/unprocessed` follows Appendix A exactly: source/limit/cursor; items contain
message_id, source, received_at, reason, retryable, attempts, status retrying/parked;
recovered messages disappear. Do not substitute canonical tombstones for DLQ state.

Errors: `{error:{code,message}}`; 400 invalid request/cursor, 401 invalid/missing
token, 403 insufficient scope, 404 confirmed absence, 409 version/authority conflict,
429 overload, 503 unavailable. JSON responses, including errors. Audit token has
no write privileges. Production also rejects forbidden source/partition assignments.

## Durability and remaining integration gates

Healthy production success requires primary and cross-node synchronous standby
durable commit. A reduced-redundancy policy must be explicit in the response and
telemetry; never describe single-copy commits as replicated. The stub cannot
validate that policy. A07/A08 establish safe failover, fencing, repair and real
placement. A09 implements bounded partial queries across shards. A02 does not
claim those runtime guarantees, C's durable 202, or completed A/B lease agreement.
