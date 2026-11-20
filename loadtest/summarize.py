#!/usr/bin/env python3
"""Aggregate k6 summary-export files for the P2 sustained/scaling test.

Criterion 6 gates on the error rate excluding controlled 429s, the number of
offered VUs, and the test duration. p95 is reported but is not a gate unless a
separate latency run sets P95_MS in k6.
"""

import argparse
import json
import sys


def metric(metrics, name):
    value = metrics.get(name)
    return value if isinstance(value, dict) else {}


def load_row(path):
    with open(path, encoding="utf-8") as fh:
        doc = json.load(fh)
    metrics = doc.get("metrics", {})
    http = metric(metrics, "http_reqs").get("count", 0) or 0
    unexpected = metric(metrics, "unexpected_errors").get("count", 0) or 0
    network = metric(metrics, "unexpected_network").get("count", 0) or 0
    fivexx = metric(metrics, "unexpected_5xx").get("count", 0) or 0
    controlled = metric(metrics, "controlled_rejections").get("count", 0) or 0
    success = metric(metrics, "successful_responses").get("count")
    if success is None:
        # Older summaries predate the explicit counter.
        success = metric(metrics, "checks").get("passes", 0) or 0
    vus = metric(metrics, "vus_max")
    p95 = metric(metrics, "http_req_duration").get("p(95)")
    error_rate = (unexpected / http) if http else (1.0 if unexpected else 0.0)
    return {
        "file": path,
        "requests": int(http),
        "successful": int(success),
        "controlled_429": int(controlled),
        "unexpected": int(unexpected),
        "network": int(network),
        "server_5xx": int(fivexx),
        "error_rate": error_rate,
        "vus_max": int(vus.get("value", 0) or 0),
        "vus_min_seen": int(vus.get("min", 0) or 0),
        "p95_ms": p95,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("files", nargs="+")
    parser.add_argument("--min-vus", type=int, default=0)
    parser.add_argument("--min-duration", type=float, default=0)
    parser.add_argument("--duration", type=float, default=0)
    parser.add_argument("--json-out")
    args = parser.parse_args()

    try:
        rows = [load_row(path) for path in args.files]
    except (OSError, ValueError) as err:
        print(f"summarize: {err}", file=sys.stderr)
        return 2

    totals = {
        "generators": len(rows),
        "requests": sum(r["requests"] for r in rows),
        "successful": sum(r["successful"] for r in rows),
        "controlled_429": sum(r["controlled_429"] for r in rows),
        "unexpected": sum(r["unexpected"] for r in rows),
        "network": sum(r["network"] for r in rows),
        "server_5xx": sum(r["server_5xx"] for r in rows),
        "vus_max": sum(r["vus_max"] for r in rows),
        "vus_min_seen": sum(r["vus_min_seen"] for r in rows),
        "duration_seconds": args.duration,
    }
    totals["error_rate"] = (
        totals["unexpected"] / totals["requests"] if totals["requests"] else 1.0
    )

    print(
        "generator                         requests  success  429s  unexpected  network  5xx  vus(max/min)      p95"
    )
    for row in rows:
        p95 = f"{row['p95_ms']:.1f}ms" if row["p95_ms"] is not None else "n/a"
        print(
            f"{row['file'][-33:]:33} {row['requests']:9d} {row['successful']:8d} "
            f"{row['controlled_429']:5d} {row['unexpected']:10d} {row['network']:8d} "
            f"{row['server_5xx']:4d} {row['vus_max']:6d}/{row['vus_min_seen']:<6d} {p95:>9}"
        )

    print(
        f"TOTAL generators={totals['generators']} requests={totals['requests']} "
        f"successful={totals['successful']} controlled_429={totals['controlled_429']} "
        f"unexpected={totals['unexpected']} network={totals['network']} "
        f"5xx={totals['server_5xx']}"
    )
    print(
        f"TOTAL vus_max={totals['vus_max']} vus_min_seen={totals['vus_min_seen']} "
        f"duration={args.duration:g}s error_rate_excl_controlled={totals['error_rate']:.6f}"
    )

    failures = []
    if totals["vus_max"] < args.min_vus:
        failures.append(
            f"offered VUs {totals['vus_max']} < required {args.min_vus}"
        )
    if args.duration < args.min_duration:
        failures.append(
            f"duration {args.duration:g}s < required {args.min_duration:g}s"
        )
    if totals["error_rate"] >= 0.01:
        failures.append(
            f"error rate {totals['error_rate']:.4%} is not below 1%"
        )

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as fh:
            json.dump({"per_generator": rows, "total": totals}, fh, indent=2)

    if failures:
        for reason in failures:
            print(f"FAIL: {reason}")
        return 1
    print("PASS: error rate below 1%, VU count and duration meet the requirement")
    return 0


if __name__ == "__main__":
    sys.exit(main())
