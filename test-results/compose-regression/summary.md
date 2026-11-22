# Compose regression results

Executed against rebuilt current source on 22 November 2026.

| Suite | Result | Evidence |
| --- | --- | --- |
| P3 | PASS; script exit 0, `All P3 checks passed.` | [p3.log](p3.log) |
| P4 controlled local | PASS; script exit 0, P4.1–P4.5 and P4.8 passed | [p4.log](p4.log) |
| P4.6 assistant reference | NOT RUN/BLOCKED; official reference absent | [p4.log](p4.log) |
| P4.7 live feed | NOT RUN; local mode uses mock feed | [p4.log](p4.log) |

P4 burst: 400 records, 200 expected fire groups, 14-second completion,
maximum sampled processing backlog 64, p95 receipt-to-store 4.494168329 seconds.
P3 recorded accepted report responses around 0.03–0.06 seconds while the
consumer was stopped, within its two-second assertion.

The first parallel cold build overloaded Docker's approximately 3.8 GiB memory
allocation. It was cancelled and services were successfully rebuilt sequentially.
No test assertion failed and no functional code revision was needed for this run.

These are Compose regression results. They do not establish Kubernetes platform
compliance, official workload/reference acceptance, or production canonical
persistence for P4 (local mode uses the contract stub).
