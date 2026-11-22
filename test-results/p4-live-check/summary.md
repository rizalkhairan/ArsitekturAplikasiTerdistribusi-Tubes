# P4.7 live practice test

Result: **PASS**, script exit 0, 180-second capture on 22 November 2026.

Actual satellite practice feed, reported speed 60x. The stored token was read
from the ignored root .env and was not printed. No exam session was started.

Started at offset 4370534, near the current feed tail rather than replaying
the full multi-million-record history. Final script sample:

| Measurement | Value |
| --- | --- |
| Durable offset | 4373750 |
| Processed offset | 4373750 |
| Feed latest offset | 4373756 |
| Ingestion lag | 6 offsets |
| Processing lag | 0 |
| Pending canonical writes | 0 |
| Orphan updates | 1 |
| p95 receipt-to-canonical-ack latency | 1.13314467 seconds |
| Reconnect attempts | 4 |

One unmatched update was counted. Starting mid-stream can omit its original
detection; this is a limitation of a tail-only live check. It was not silently
treated as a successfully applied correction.

This proves live-feed processing for the observed interval. Canonical writes
went to the local contract stub, and province mapping used the synthetic local
fixture. It does not prove production canonical persistence, complete-history
accuracy, or the official reference comparison. P4.6 was skipped because its
official reference file was not supplied. Reconnect count is observational;
forced reconnect was tested separately in the controlled Compose suite.

See [live.log](live.log), [session.json](session.json), and
[progress.json](progress.json) for saved evidence.
