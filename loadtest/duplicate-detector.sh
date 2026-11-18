#!/usr/bin/env bash
# P2 exactly-once evidence.
#
# Reads the upstream mock request logs (caller instance + timestamp) and the
# canonical storage rows, then asserts:
#   1. no duplicate (source, source_ref_id) rows;
#   2. no two Aggregator instances are logged as successful callers for the same
#      source in the same one-second bucket (handover gaps are allowed, duplicate
#      work is not).
set -euo pipefail

PYTHON="${PYTHON:-}"
if [ -z "$PYTHON" ]; then
  if python3 -c 'import sys' >/dev/null 2>&1; then PYTHON=python3; else PYTHON=python; fi
fi

NS="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
OUT="${OUT:-loadtest/results}"
mkdir -p "$OUT"

cleanup() {
  kill "${STORAGE_PF:-0}" "${BMKG_PF:-0}" "${PVMBG_PF:-0}" 2>/dev/null || true
}
trap cleanup EXIT

kubectl -n "$NS" port-forward svc/canonical-store 18095:8080 >/dev/null 2>&1 &
STORAGE_PF=$!
kubectl -n "$NS" port-forward svc/bmkg 18083:8083 >/dev/null 2>&1 &
BMKG_PF=$!
kubectl -n "$NS" port-forward svc/pvmbg 18082:8082 >/dev/null 2>&1 &
PVMBG_PF=$!
sleep 5

curl -fsS http://127.0.0.1:18095/events > "$OUT/detector-events.json"
curl -fsS http://127.0.0.1:18083/admin/requests > "$OUT/detector-bmkg.json"
curl -fsS http://127.0.0.1:18082/admin/requests > "$OUT/detector-pvmbg.json"

"$PYTHON" - "$OUT" <<'PY'
import json, sys, collections, datetime

out = sys.argv[1]
events = json.load(open(f"{out}/detector-events.json"))
bmkg = json.load(open(f"{out}/detector-bmkg.json"))
pvmbg = json.load(open(f"{out}/detector-pvmbg.json"))

duplicates = collections.Counter((e.get("source"), e.get("source_ref_id")) for e in events)
dup_rows = {k: v for k, v in duplicates.items() if v > 1}
print(f"storage rows={len(events)} duplicate_keys={len(dup_rows)}")
for key, count in list(dup_rows.items())[:10]:
    print("  duplicate", key, count)

def buckets(records, key_field):
    grouped = collections.defaultdict(set)
    for r in records:
        ts = r.get("ts", "")
        try:
            second = datetime.datetime.fromisoformat(ts.replace("Z", "+00:00")).replace(microsecond=0).isoformat()
        except ValueError:
            continue
        caller = r.get("caller_instance", "unknown")
        if caller in ("", "unknown"):
            continue
        grouped[(r.get(key_field, ""), second)].add(caller)
    return {k: v for k, v in grouped.items() if len(v) > 1}

bmkg_splits = buckets(bmkg, "method")
pvmbg_splits = buckets(pvmbg, "path")
print(f"bmkg same-second multi-caller buckets={len(bmkg_splits)}")
for k, v in list(bmkg_splits.items())[:10]:
    print("  bmkg", k, sorted(v))
print(f"pvmbg same-second multi-caller buckets={len(pvmbg_splits)}")
for k, v in list(pvmbg_splits.items())[:10]:
    print("  pvmbg", k, sorted(v))

if dup_rows or bmkg_splits or pvmbg_splits:
    print("RESULT: FAIL")
    sys.exit(1)
print("RESULT: PASS")
PY
