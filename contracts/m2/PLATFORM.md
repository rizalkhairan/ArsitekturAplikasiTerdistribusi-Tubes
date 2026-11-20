# Platform and telemetry conventions

Integration baseline for deployment and shared instrumentation. The official
M2 PDF controls namespace, role labels, Secret/ConfigMap usage, resource classes,
Tester Agent access, and observability outcomes. Names, ports, metrics and helper
APIs below are team choices. Backend deployment and full-service adoption are
separate work; these files do not prove cluster observability or storage isolation.

## Namespace, identity and scheduling

Application namespace: `sistem-informasi-kebencanaan-bnpb`. Use the following
labels on each workload's **pod template**, not only the Deployment metadata:

| Label | Convention |
| --- | --- |
| `app.kubernetes.io/name` | Stable component name from the service table below |
| `app.kubernetes.io/part-of` | `bnpb` |
| `app.kubernetes.io/component` | `aggregator`, `database`, `broker`, `processor`, `api`, `mock`, `sensor`, or `telemetry` |
| `role` | Exactly one official value: `aggregator`, `store`, `broker`, `consumer`, `api`, `mock`, `sensor`, `other` |
| `bnpb.io/role` | Node role contract: `control`, `app` or `store`; use `role=store` on Citus pods |

Service selectors use name plus part-of. Never select by version, pod name or
correlation ID. Node eligibility uses `bnpb.io/role` plus
`bnpb.io/app-eligible=true` on app/store; the compatibility labels
`app-eligible`, `store-eligible` and `aggregator-eligible` may remain for older
workloads. Avoid hard anti-affinity that blocks the 3→5 Aggregator scaling test.
Add downward API env `POD_NAME=metadata.name` and `NODE_NAME=spec.nodeName`; set
`OTEL_SERVICE_NAME` to the stable component name.

Tester Agent stays in its supplied namespace, unchanged. Do not apply policy,
RBAC, taints or resource constraints that obstruct its required access.

## Service DNS and ports

Short DNS names below resolve inside the application namespace. Cross-namespace
callers use `<service>.sistem-informasi-kebencanaan-bnpb.svc.cluster.local`.
This table reserves endpoints; it does not assert the Services already exist.

| Service / component | Pod role | Port and purpose |
| --- | --- | --- |
| `bnpb` | aggregator | TCP 8085 canonical/internal/Audit API; 8080 existing health server |
| `client-api` | api | TCP 8080 user HTTP entry point |
| `auth` | api | TCP 8000 authentication |
| `bmkg` | mock | TCP 8081 gRPC |
| `pvmbg` | mock | TCP 8082 HTTP |
| `kafka` | broker | TCP 9092 bootstrap; controller listener stays private |
| `dashboard-updater`, `notifier`, `analytics-consumer` | consumer | TCP 8080 existing health endpoints |
| `satellite-processor` | broker | TCP 8080 health; consumes external feed |
| `field-report-consumer` | consumer | TCP 8080 health |
| `sensor-pantai` | sensor | Use the supplied sensor's listener/configuration; do not invent its API |
| `citus-coordinator` | store | TCP 5432 Citus coordinator SQL endpoint |
| `citus-worker-1`, `citus-worker-2`, `citus-worker-3` | store | TCP 5432 Citus worker PostgreSQL endpoints |
| `canonical-api` (Service alias `canonical-store` for legacy scripts) | other | TCP 8080 canonical adapter/audit/analytics API |
| `otel-collector` | other | TCP 4318 OTLP/HTTP protobuf ingestion |
| `prometheus`, `grafana`, `tempo`, `loki` | other | TCP 9090, 3000, 3200, 3100 respectively |

Instrumented application pods reserve named port `metrics`, TCP 9090, serving
`GET /metrics` on an internal management listener. BMKG uses this separate HTTP
listener alongside gRPC. Prometheus itself already uses 9090 in its own pod.
Exporter-backed components keep their native metrics ports when necessary;
deployment discovery must explicitly configure those targets. Management ports
are cluster-internal; the public client entry point does not proxy `/metrics`.
Telemetry system receivers/exporters may need additional internal ports when
their manifests are implemented. Do not expose PostgreSQL/Citus or Kafka
directly to browsers/CLI users.

## Configuration and credential names

[Deployment platform resources](../../deploy/kubernetes/base/platform.yaml)
provide the Namespace and two nonsecret ConfigMaps. Import only
the keys a workload uses with `configMapKeyRef`; do not inject every endpoint into
every pod. `bnpb-platform` contains stable service addresses. `bnpb-telemetry`
contains export configuration and the metrics bind address. Per-component tuning
belongs to `<component>-config`; broker topics, DLQ and pipeline options are C's
contracts, not silently renamed by this file. Keep M1 `.env.example` files for local
development; `services/shared/.env.example` covers the new helper/example.

Secrets contain no committed real values. Reserve these names and keys:

| Secret | Keys | Who receives it |
| --- | --- | --- |
| `bnpb-audit-auth` | `token` | Aggregator Audit middleware only |
| `bnpb-canonical-cluster` | `PATRONI_SUPERUSER_PASSWORD` (legacy key name; no Patroni deployment), `CANONICAL_APP_PASSWORD` | Citus StatefulSets and `citus-init` |
| `bnpb-citus-admin` | `CANONICAL_ADMIN_DSN` | `citus-init` schema Job and the `replica-reconciler` sidecar in canonical-api |
| `bnpb-citus-api` | `CANONICAL_DSN` | `canonical-api` adapter |
| `bnpb-bmkg-auth` | `api-key` | BMKG mock and authorized Aggregator poller |
| `bnpb-pvmbg-auth` | `token` | PVMBG mock and authorized Aggregator poller |
| `bnpb-jwt-auth` | `signing-key` | Auth and Client-Facing API |
| `bnpb-<caller>-canonical-auth` | `token` | Caller and Aggregator's credential verifier |
| `bnpb-<component>-db` | `username`, `password`, `database` | Owning service and its private database provisioning only |

`<caller>` is an actual workload such as `satellite-processor`; use a distinct
credential per service and assigned source/partition. B chooses its coordination
store credential details; C chooses broker/pipeline credentials. Do not reuse the
Audit token, upstream credentials or JWT key for canonical writes. M1 shared
internal keys need an explicit migration when per-service auth is implemented.
NetworkPolicies, volume ownership and database grants must enforce boundaries
in deployment; merely naming Secrets does not enforce them. Store pods use local
PV/hostPath on their hosting node. No shared volume mounted into other services.

Appendix B requests/limits per pod remain: Aggregator 256Mi/512Mi; store and
broker/stream processor 512Mi/1Gi; consumer/event processor, auth/API and other
128Mi/256Mi; BMKG/PVMBG mocks 64Mi/128Mi. Apply the sensor's documented class
when integrating its supplied deployment. Count replicas and telemetry in the
capacity budget. These are required declarations, not measured usage guarantees.

## Logs, traces and asynchronous context

Use `services/shared/telemetry` for Go. It emits one JSON object per stdout line
through `slog`: `time`, `level`, `msg`, `service`, `instance`, `environment`,
`correlation_id`, and valid `trace_id`/`span_id`. `time` is RFC3339 with offset;
configure hosts to UTC for consistent display. HTTP completion adds `route`,
`method`, `status_code`, `duration_seconds`. New code uses `correlation_id`;
do not retain M1's competing `corr_id` spelling in newly instrumented paths.
Never log tokens, passwords, authorization headers, raw request bodies or full
upstream URLs with query secrets. IDs belong in logs/spans, never metric labels.

HTTP uses `X-Correlation-ID`, W3C `traceparent` and `tracestate`. Accept correlation
IDs of 1–128 ASCII letters/digits/`._-`, replace others with random IDs, return the
effective correlation ID in the response, and forward it downstream. Tracing
accepts only the valid W3C context; a correlation ID alone is not a trace.
Baggage is deliberately not propagated. The helper uses explicit providers;
callers using another instrumentor must pass this provider/propagator rather than
expecting a global OpenTelemetry installation.

For broker messages, convert a `propagation.MapCarrier` into UTF-8 Kafka headers
named `traceparent`, optional `tracestate`, and `x-correlation-id`. Persist this
carrier with the durable outbox payload so publisher retries and restarts preserve
causality. Never save a Go `context.Context`. On publish, extract persisted
context, start a producer span, then inject that span's context into headers. On
consume, extract once, start a consumer span, log/perform work, then end the span.
Deduplication remains based on message/event identity, never correlation ID.
Each retry has its own processing span. Do not change original ingestion time.
For batches, preserve each record's carrier; use span links for unrelated parents.

For BMKG gRPC, adapt lowercase metadata to `propagation.TextMapCarrier`; inject
outgoing metadata and extract incoming metadata, then start client/server spans
with this provider. Register interceptors/stats handlers when BMKG is integrated.
The shared helper supplies transport-neutral propagation, not automatic gRPC or
Kafka client installation. Keep metric and source labels consistent across those
protocols.

`OTEL_TRACES_EXPORTER=otlp` selects the OTLP/HTTP exporter; standard
`OTEL_EXPORTER_OTLP_*` variables configure its endpoint/TLS/headers. The ConfigMap
uses `http://otel-collector:4318`, protocol `http/protobuf`. Local mode is `none`
(default) or `stdout` (spans on stderr). `BNPB_TRACE_SAMPLE_RATIO` is 0..1;
demo default 1 retains all locally started traces. Parent sampling is respected.
Always flush with a bounded shutdown context. Export runs in a bounded background
batch queue; backend failure must not become application storage failure.

## Metrics and dashboard queries

| Metric | Type | Labels |
| --- | --- | --- |
| `bnpb_http_requests_total` | Counter | service, route, method, status_code |
| `bnpb_http_request_duration_seconds` | Histogram | service, route, method |
| `bnpb_http_requests_in_flight` | Gauge | service |

Helpers register Go/process collectors on an isolated registry. Scrapes/probes
are mounted outside application instrumentation. `route` is a developer-supplied
constant template such as `GET /hazards/{id}`; fallback is `unmatched`. The helper
normalizes unknown methods to `OTHER`. Never supply URL paths containing IDs,
source_ref_id, hazard_id, correlation IDs, arbitrary exception text or user input
as labels. New collectors register on `Runtime.Registry`, not the global registry.
Prometheus discovery supplies pod/node/namespace metadata from Kubernetes.

PromQL (5xx is the server-error definition; 4xx remains visible by status):

```promql
sum by (service) (rate(bnpb_http_requests_total[5m]))
sum by (service) (rate(bnpb_http_requests_total{status_code=~"5.."}[5m]))
  / clamp_min(sum by (service) (rate(bnpb_http_requests_total[5m])), 0.000001)
histogram_quantile(0.95, sum by (service, le) (rate(bnpb_http_request_duration_seconds_bucket[5m])))
```

B/C additionally instrument upstream attempts/outcomes, circuit state, notification
latency, broker lag, DLQ state, and event-time processing lag when implementing
their components. Name duration histograms with `_seconds`, counters with `_total`;
use bounded source/outcome labels. This baseline does not invent numeric evidence
or claim the HTTP metrics alone cover background workers or BMKG gRPC.

## Adopt and run

See [shared helper guide](../../services/shared/README.md) for the two-service
example and module/build instructions. Replace M1 HTTP logging/correlation
middleware when adopting the shared handler to avoid duplicate request logs and
competing IDs. Component owners instrument their own paths. The operational
dashboard, cross-service trace viewer and correlation-ID log search are deployed
with the telemetry stack; the P1 analytics dashboard is a separate product view.

References: [OpenTelemetry Go](https://opentelemetry.io/docs/languages/go/getting-started/),
[HTTP instrumentation](https://pkg.go.dev/go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp),
[Prometheus metric naming](https://prometheus.io/docs/practices/naming/).
