# Load tests (Person B)

Tools run **outside the application pods**. k6 is launched from the host or
from external Linux VMs; no Kubernetes Job, `kubectl run`, or load-generator pod
is used. The only cluster-side artifact is a temporary NodePort Service for the
direct Aggregator throughput/rolling tests, deleted on exit.

## Prerequisites

- `k6` (v1.x) on the generator host(s)
- `kubectl` pointed at the demo cluster
- `python3` or `python`
- `ssh`/`scp` only when `GENERATOR_HOSTS` is used
- external generator VMs must be able to reach the client-api/Aggregator
  NodePort on the host-only network

## Preflight

```bash
BASE_URL=http://192.168.56.10:30081 ./loadtest/preflight.sh
```

It checks Ready nodes, Running pods, Aggregator 3/3, the external client-api
endpoint, and k6/python on remote generators. Run it on a quiet cluster: no
image import, no rollout, and no other heavy test in progress.

## Criterion 6 — sustained 10k VUs + scaling 3 -> 5 -> 2

The requirement gates on:

- aggregate `error_rate_excl_controlled < 0.01`
- at least 10,000 offered VUs for at least 120 seconds
- criterion 2 (duplicate detector) still passes

Latency is reported but is not a gate. The old `p95 < 1s` threshold is now
optional via `P95_MS`; the assignment does not include it in criterion 6.

One external generator (smoke):

```bash
BASE_URL=http://192.168.56.10:30081 \
VUS=1000 DURATION=120s ALLOW_SMALL=1 RUN_DETECTOR=0 \
./loadtest/scaling.sh
```

External generators totaling 10k VUs (recommended):

```bash
BASE_URL=http://192.168.56.10:30081 \
GENERATOR_HOSTS="ubuntu@192.168.56.20 ubuntu@192.168.56.21" \
VUS=10000 DURATION=120s \
./loadtest/scaling.sh
```

Each generator gets a share of the VUs (the script distributes the remainder so
the total is exactly 10,000). The orchestrator scales the Aggregator 3 -> 5 at
+20s and 5 -> 2 at +60s, waits for all generators, aggregates the summaries, and
runs the detector. `scaling-aggregate.json` and `scaling-detector.txt` are the
evidence.

Useful variables:

| Variable | Default | Meaning |
|---|---|---|
| `VUS` | `10000` | Total offered VUs |
| `DURATION` | `120s` | Test duration |
| `GENERATOR_HOSTS` | empty | Space-separated SSH targets for external k6 |
| `LOCAL_GENERATORS` | `1` if no hosts | Number of local k6 processes |
| `VUS_PER_GENERATOR` | unset | Overrides `VUS`, total = this × generator count |
| `ALLOW_SMALL` | `0` | `1` disables the 10k/120s gate for smoke tests |
| `RUN_DETECTOR` | `1` | Run the duplicate detector after the load |
| `RAW_OUT` | `0` | `1` writes local k6 raw JSON (large; not needed for evidence) |
| `SCALE_UP_AT` / `SCALE_DOWN_AT` | `20` / `60` | Scaling timestamps in seconds |

Do not add `--out json` to the 10k run unless you need raw request data; it adds
significant generator I/O. The summary is the evidence.

## Criterion 7 — throughput 3 instances >= 2.0x 1 instance

`throughput.sh` creates a temporary NodePort Service (`aggregator-load`,
default port `30085`) so external k6 can reach the Aggregator without
`kubectl port-forward` (a single tunnel is a bottleneck). The Service is deleted
on exit.

Calibrate first: find the VU count where one replica plateaus, then use that same
count for both legs.

```bash
BASE_URL=http://192.168.56.10:30081 \
CALIBRATE=1 ./loadtest/throughput.sh

VUS=2000 DURATION=60s ./loadtest/throughput.sh
```

The pass gate is the ratio of **successful 200 responses per second**:
`three / one >= 2.0`. Both legs must have <1% unexpected errors for the
measurement to be valid; controlled 429s are reported but do not inflate the
successful throughput.

If you have an existing direct endpoint, set `AGG_URL` to skip the temporary
Service.

## Criterion 8 — rolling version change without failed requests

Criterion 8 does not mandate 10k VUs. Use the highest load that yields zero
controlled 429s and zero unexpected errors; 300–500 VUs is usually sufficient to
have requests in flight throughout the rollout.

The v2 image must already be on every node. Without Docker, create a distinct
test version tag from the existing image:

```bash
./scripts/tag-aggregator-version.sh
# creates docker.io/bnpb-gateway/aggregator:m2-v2 on every node
```

If you have a genuinely different build, import it under a new tag instead.

```bash
VUS=500 DURATION=120s ROLLOUT_AT=30 \
NEW_IMAGE=bnpb-gateway/aggregator:m2-v2 \
./loadtest/rolling-update.sh
```

The script starts k6, replaces the image with `kubectl set image` at +30s, waits
for the rollout, and gates on:

- `unexpected_errors = 0`
- `controlled_rejections = 0` (set `ALLOW_CONTROLLED=1` to relax this)
- all Aggregator pods Running the new image

The deployment already has `preStop: sleep 5`, `terminationGracePeriodSeconds:
30`, `maxUnavailable: 0`, and `maxSurge: 1`.

## Exactly-once detector

```bash
./loadtest/duplicate-detector.sh
```

It reads the upstream mock request logs and canonical storage rows; it is safe
to run after any of the load tests and is included automatically by
`scaling.sh` when `RUN_DETECTOR=1`.

## Notes

- The scripts raise `TOKEN_TTL_SECONDS` on `client-api` and `auth` to 1 hour;
  set `SKIP_TTL_ENV=1` to skip that.
- `controlled_rejections` (429 from admission control) are excluded from the
  criterion-6 `<1%` error rate; `unexpected_errors` are not.
- Raw output and summaries are written to `loadtest/results/`, which is
  gitignored except for its README. Paste the summaries, aggregate JSON, pod
  lists, and detector output into the report.
