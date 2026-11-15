# Shared Go telemetry

Module: `github.com/rizalkhairan/distributed-system/shared`. Supplies structured
JSON logging, Prometheus metrics, OpenTelemetry server/client spans, and shared
context propagation. [Platform conventions](../../contracts/m2/PLATFORM.md) define
cluster names, ports, credentials and telemetry fields. Existing services adopt
this module when their instrumentation is integrated; this change adds no implicit
global providers or middleware to the running M1 applications.

From another `services/<component>` Go module:

```go
require github.com/rizalkhairan/distributed-system/shared v0.0.0
replace github.com/rizalkhairan/distributed-system/shared => ../shared
```

Import `github.com/rizalkhairan/distributed-system/shared/telemetry`. Call
`telemetry.FromEnv(ctx)` once; mount `t.Handler("GET /hazards/{id}", handler)`
using a constant route pattern, and mount `t.MetricsHandler()` on an internal
HTTP management server. Use `t.Transport(nil)` in a timeout-configured client,
`http.NewRequestWithContext`, `t.Log(ctx)`, and `t.Tracer.Start` for work spans.
Pass `t.Provider` explicitly to additional OTel instrumentors. Always close
HTTP response bodies and call `t.Shutdown` with a bounded context on exit.

Docker builds adopting this local replacement must use `services/` as context,
copy `shared/go.mod` and `shared/go.sum` before dependency resolution, and copy
the shared source before compiling. Update the component's Compose build context
and Dockerfile together; existing component images remain unchanged until adoption.

Run two example processes from `services/shared`, one terminal each:

```bash
OTEL_SERVICE_NAME=example-worker OTEL_TRACES_EXPORTER=stdout \
  EXAMPLE_ADDR=127.0.0.1:8097 METRICS_ADDR=127.0.0.1:9097 \
  go run ./cmd/telemetry-example
```

```bash
OTEL_SERVICE_NAME=example-api OTEL_TRACES_EXPORTER=stdout \
  EXAMPLE_UPSTREAM_URL=http://127.0.0.1:8097 \
  go run ./cmd/telemetry-example
```

Then:

```bash
curl -i -H 'X-Correlation-ID: example-request-1' http://127.0.0.1:8096/demo
curl -s http://127.0.0.1:9096/metrics
curl -s http://127.0.0.1:9097/metrics
```

Both processes log the same correlation/trace IDs with different span IDs.
After Ctrl-C, batched spans flush to stderr, showing API server → HTTP client →
worker server → compute parentage. Traces can also flush periodically while
running. Use `OTEL_TRACES_EXPORTER=otlp` and an available OTLP/HTTP collector to
inspect the trace in a backend; no collector is required for stdout mode.

For asynchronous carriers:

```go
headers := propagation.MapCarrier{}
t.Inject(ctx, headers) // persist strings in outbox; convert to Kafka headers
// On the receiver, after reconstructing headers:
ctx = t.Extract(context.Background(), headers)
ctx, span := t.Tracer.Start(ctx, "hazard.consume", trace.WithSpanKind(trace.SpanKindConsumer))
defer span.End()
t.Log(ctx).Info("hazard_processed")
```

Test/build from this module with `go test -race ./...` and `go vet ./...`.
Tests exercise HTTP parentage, carrier propagation, metric cardinality, response
status/streaming behavior, and structured logging. The example is local sample
instrumentation; running it does not verify production component adoption.
