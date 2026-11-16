# Load tests (Person B)

Tools run **outside** the application pods. The k6 scripts target the
Client-Facing API Service.

## Prerequisites

- `k6` (v1.x)
- `kubectl` pointed at the demo cluster
- `python3` (for summary parsing)
- `jq` optional

## Commands

```bash
# P2 sustained: 10,000 VUs / 120 s while scaling 3 -> 5 -> 2
./loadtest/scaling.sh

# P2 throughput: 1 vs 3 instances, target >= 2.0x
./loadtest/throughput.sh

# P2 rolling update with zero failed requests
./loadtest/rolling-update.sh

# P2 exactly-once detector (reads mock request logs + storage rows)
./loadtest/duplicate-detector.sh

# P5 priority lane under overload (field vs media)
k6 run -e FIELD_VUS=4000 -e MEDIA_VUS=4000 loadtest/k6/priority.js
```

Raw output is written to `loadtest/results/` and is gitignored except for the
README. Paste the relevant `*-summary.json`, `*-pods.txt`, and detector output
into the report.

## Notes

- The scripts raise `TOKEN_TTL_SECONDS` on `client-api` to 1 hour for the run.
  The production TTL remains 60 s; token expiry is tested separately in M1
  scenario 3c.
- `controlled_rejections` (429 from admission control) are excluded from the
  `<1%` error-rate criterion; `unexpected_errors` are not.
- If the cluster cannot sustain 10k VUs on one generator, run k6 from several
  external machines with the same script and aggregate the summaries. Do not run
  k6 inside an application pod.
