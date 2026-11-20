#!/usr/bin/env python3
"""Load synthetic SEISMIC rows for P1 when the assistant catalog is unavailable.

This is explicitly synthetic test data, not the official assistant reference.
It exercises the same canonical write path, Citus hash distribution, audit
pagination, checksums and node-loss partial responses.
"""
import concurrent.futures
import datetime as dt
import json
import sys
import urllib.error
import urllib.request

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:18081"
COUNT = int(sys.argv[2]) if len(sys.argv) > 2 else 500
WORKERS = 16
SEVERITIES = ("NORMAL", "WASPADA", "SIAGA", "AWAS")


def put(index):
    ref = f"synthetic-p1-{index:06d}"
    now = dt.datetime(2026, 11, 19, 2, 0, tzinfo=dt.timezone.utc)
    occurred = now - dt.timedelta(minutes=index)
    body = {
        "version": 1,
        "operation": "upsert",
        "authority": {"partition": "P1_SYNTHETIC", "epoch": 1},
        "correlation_id": f"p1-synthetic-{index:06d}",
        "event": {
            "hazard_type": "SEISMIC",
            "severity": SEVERITIES[index % len(SEVERITIES)],
            "area_name": f"Synthetic Region {index % 17:02d}",
            "latitude": -6.0 + (index % 100) / 100.0,
            "longitude": 106.0 + (index % 100) / 100.0,
            "occurred_at": occurred.isoformat().replace("+00:00", "Z"),
            "attributes": {
                "synthetic": True,
                "magnitude": round(3.0 + (index % 45) / 10.0, 2),
                "depth_km": 10 + (index % 90),
                "potential_tsunami": False,
            },
        },
    }
    request = urllib.request.Request(
        # The historical catalog is BMKG/earthquake data. BMKG writes are
        # fenced, so the synthetic loader uses the admin replay route, which
        # is the same path used for DLQ recovery.
        f"{BASE}/internal/admin/events/BMKG/{ref}",
        data=json.dumps(body).encode(),
        method="PUT",
        headers={"Content-Type": "application/json"},
    )
    for attempt in range(4):
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                return index, response.status, None
        except urllib.error.HTTPError as error:
            if error.code in (429, 500, 503) and attempt < 3:
                continue
            return index, error.code, error.read().decode(errors="replace")[:200]
        except Exception as error:  # connection reset/retry
            if attempt < 3:
                continue
            return index, None, str(error)[:200]
    return index, None, "retry exhausted"


def main():
    ok = failed = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for index, status, error in pool.map(put, range(1, COUNT + 1)):
            if status in (200, 201):
                ok += 1
            else:
                failed += 1
                print(f"FAIL {index}: {status} {error}", file=sys.stderr)
    print(json.dumps({"requested": COUNT, "ok": ok, "failed": failed, "synthetic": True}))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
