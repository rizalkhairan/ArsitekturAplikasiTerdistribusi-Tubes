#!/usr/bin/env bash
# P2 criterion 6: sustained >=10,000 VUs for >=120 s while the Aggregator is
# scaled 3 -> 5 -> 2.
#
# k6 runs outside the application pods. Use one or more external Linux VMs:
#   GENERATOR_HOSTS="ubuntu@192.168.56.20 ubuntu@192.168.56.21" \
#   VUS=10000 BASE_URL=http://192.168.56.10:30081 ./loadtest/scaling.sh
#
# Or run several local k6 processes (useful for a smoke test, but one host's
# network stack is still a bottleneck):
#   LOCAL_GENERATORS=4 VUS=10000 BASE_URL=... ./loadtest/scaling.sh
#
# The pass gate is the criterion from the assignment: aggregate error rate
# excluding controlled 429s < 1%, the configured VU count is met, and the
# duplicate detector still passes. p95 is reported but is not a gate unless the
# optional P95_MS environment variable is set for a separate latency run.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=loadtest/lib.sh
source "$ROOT/loadtest/lib.sh"

NS="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
BASE_URL="${BASE_URL:-http://192.168.56.10:30081}"
OUT="${OUT:-$ROOT/loadtest/results}"
VUS="${VUS:-10000}"
DURATION="${DURATION:-120s}"
SLEEP="${SLEEP:-0.05}"
INTERNAL_KEY="${INTERNAL_KEY:-}"
TOKEN="${TOKEN:-}"
MIN_VUS="${MIN_VUS:-10000}"
MIN_DURATION="${MIN_DURATION:-120}"
ALLOW_SMALL="${ALLOW_SMALL:-0}"
RUN_DETECTOR="${RUN_DETECTOR:-1}"
RAW_OUT="${RAW_OUT:-0}"
SCALE_UP_AT="${SCALE_UP_AT:-20}"
SCALE_DOWN_AT="${SCALE_DOWN_AT:-60}"
REMOTE_SCRIPT="${REMOTE_SCRIPT:-/tmp/bnpb-sustained.js}"
REMOTE_OUT="${REMOTE_OUT:-/tmp/bnpb-loadtest}"

pick_python
mkdir -p "$OUT"

parse_seconds() {
  case "$1" in
    *ms) echo $(( ${1%ms} / 1000 )) ;;
    *s)  echo "${1%s}" ;;
    *m)  echo $(( ${1%m} * 60 )) ;;
    *h)  echo $(( ${1%h} * 3600 )) ;;
    *)   echo "$1" ;;
  esac
}

DURATION_SECONDS="$(parse_seconds "$DURATION")"
if ! [[ "$DURATION_SECONDS" =~ ^[0-9]+$ ]]; then
  echo "[load] invalid DURATION=$DURATION" >&2
  exit 2
fi
if ! [[ "$VUS" =~ ^[0-9]+$ ]] || [ "$VUS" -lt 1 ]; then
  echo "[load] invalid VUS=$VUS" >&2
  exit 2
fi
if [ "$SCALE_DOWN_AT" -le "$SCALE_UP_AT" ]; then
  echo "[load] SCALE_DOWN_AT must be greater than SCALE_UP_AT" >&2
  exit 2
fi

GATE_MIN_VUS="$MIN_VUS"
GATE_MIN_DURATION="$MIN_DURATION"
if [ "$ALLOW_SMALL" = "1" ]; then
  GATE_MIN_VUS=1
  GATE_MIN_DURATION=1
fi

# ----- generator layout ----------------------------------------------------
hosts=()
if [ -n "${GENERATOR_HOSTS:-}" ]; then
  read -r -a hosts <<< "$GENERATOR_HOSTS"
fi
remote_count=${#hosts[@]}
local_count="${LOCAL_GENERATORS:-0}"
if ! [[ "$local_count" =~ ^[0-9]+$ ]]; then
  echo "[load] LOCAL_GENERATORS must be a number" >&2
  exit 2
fi
if [ "$remote_count" -eq 0 ] && [ "$local_count" -eq 0 ]; then
  local_count=1
fi
if [ "$local_count" -gt 0 ]; then
  require_k6_local
fi
total_generators=$((remote_count + local_count))

gen_label=()
gen_kind=()
gen_target=()
gen_vus=()

if [ -n "${VUS_PER_GENERATOR:-}" ]; then
  if ! [[ "$VUS_PER_GENERATOR" =~ ^[0-9]+$ ]] || [ "$VUS_PER_GENERATOR" -lt 1 ]; then
    echo "[load] invalid VUS_PER_GENERATOR" >&2
    exit 2
  fi
  VUS=$((VUS_PER_GENERATOR * total_generators))
  for ((i = 0; i < total_generators; i++)); do
    gen_vus+=("$VUS_PER_GENERATOR")
  done
else
  base=$((VUS / total_generators))
  remainder=$((VUS % total_generators))
  for ((i = 0; i < total_generators; i++)); do
    if [ "$i" -lt "$remainder" ]; then
      gen_vus+=("$((base + 1))")
    else
      gen_vus+=("$base")
    fi
  done
fi

idx=0
for ((i = 1; i <= local_count; i++)); do
  gen_label+=("local$i")
  gen_kind+=("local")
  gen_target+=("")
  idx=$((idx + 1))
done
for host in "${hosts[@]}"; do
  safe_host="$(printf '%s' "$host" | tr -c 'A-Za-z0-9_.-' '_')"
  gen_label+=("remote$((idx + 1))-$safe_host")
  gen_kind+=("remote")
  gen_target+=("$host")
  idx=$((idx + 1))
done

for host in "${hosts[@]}"; do
  ssh -o BatchMode=yes "$host" 'command -v k6 >/dev/null 2>&1' || {
    echo "[load] k6 is not available on $host" >&2
    exit 1
  }
  ssh -o BatchMode=yes "$host" "mkdir -p '$REMOTE_OUT'"
  scp -q "$ROOT/loadtest/k6/sustained.js" "$host:$REMOTE_SCRIPT"
done

raise_token_ttl
kubectl -n "$NS" scale deployment/aggregator --replicas=3 >/dev/null
kubectl -n "$NS" rollout status deployment/aggregator --timeout=180s >/dev/null

cleanup() {
  restore_aggregator_replicas
}
trap cleanup EXIT

echo "[load] generators=$total_generators local=$local_count remote=$remote_count"
echo "[load] total VUs=$VUS per-generator=${gen_vus[*]} duration=$DURATION ($DURATION_SECONDS s)"
echo "[load] target=$BASE_URL"
if [ "$ALLOW_SMALL" = "1" ]; then
  echo "[load] ALLOW_SMALL=1: the 10k/120s gate is disabled for this run"
fi

# ----- launch k6 outside the application pods -----------------------------
pids=()
for i in "${!gen_label[@]}"; do
  label="${gen_label[$i]}"
  v="${gen_vus[$i]}"
  kind="${gen_kind[$i]}"
  target="${gen_target[$i]}"
  env_args=(
    -e "BASE_URL=$BASE_URL"
    -e "VUS=$v"
    -e "DURATION=$DURATION"
    -e "SLEEP=$SLEEP"
    -e "INTERNAL_KEY=$INTERNAL_KEY"
    -e "TOKEN=$TOKEN"
  )
  [ -n "${K6_USER:-}" ] && env_args+=(-e "K6_USER=$K6_USER")
  [ -n "${K6_PASSWORD:-}" ] && env_args+=(-e "K6_PASSWORD=$K6_PASSWORD")
  [ -n "${P95_MS:-}" ] && env_args+=(-e "P95_MS=$P95_MS")

  if [ "$kind" = "local" ]; then
    out_args=()
    if [ "$RAW_OUT" = "1" ]; then
      out_args+=(--out "json=$OUT/scaling-$label-raw.json")
    fi
    k6 run "${env_args[@]}" \
      --summary-export "$OUT/scaling-$label-summary.json" \
      "${out_args[@]}" "$ROOT/loadtest/k6/sustained.js" \
      > "$OUT/scaling-$label.log" 2>&1 &
  else
    # The remote command uses the same external k6, not a pod.
    remote_cmd="cd '$REMOTE_OUT' && env BASE_URL='$BASE_URL' VUS='$v' DURATION='$DURATION' SLEEP='$SLEEP' INTERNAL_KEY='$INTERNAL_KEY' TOKEN='$TOKEN'"
    [ -n "${K6_USER:-}" ] && remote_cmd="$remote_cmd K6_USER='$K6_USER'"
    [ -n "${K6_PASSWORD:-}" ] && remote_cmd="$remote_cmd K6_PASSWORD='$K6_PASSWORD'"
    [ -n "${P95_MS:-}" ] && remote_cmd="$remote_cmd P95_MS='$P95_MS'"
    remote_cmd="$remote_cmd k6 run --summary-export '$REMOTE_OUT/scaling-$label-summary.json' '$REMOTE_SCRIPT'"
    ssh -o BatchMode=yes "$target" "$remote_cmd" > "$OUT/scaling-$label.log" 2>&1 &
  fi
  pids+=("$!")
  echo "[load] started $label ($kind${target:+, $target}) vus=$v pid=$!"
done

sleep 2
if [ "$SCALE_UP_AT" -gt 2 ]; then
  sleep $((SCALE_UP_AT - 2))
fi
echo "[load] scale 3 -> 5 at +$(date -u +%H:%M:%S)"
kubectl -n "$NS" scale deployment/aggregator --replicas=5
remain=$((SCALE_DOWN_AT - SCALE_UP_AT))
if [ "$remain" -gt 0 ]; then
  sleep "$remain"
fi
echo "[load] scale 5 -> 2 at +$(date -u +%H:%M:%S)"
kubectl -n "$NS" scale deployment/aggregator --replicas=2

echo "[load] waiting for generators"
set +e
for i in "${!pids[@]}"; do
  wait "${pids[$i]}"
  status=$?
  echo "[load] ${gen_label[$i]} k6 exit=$status (threshold exit is informational)"
done
set -e

for i in "${!gen_label[@]}"; do
  if [ "${gen_kind[$i]}" = "remote" ]; then
    target="${gen_target[$i]}"
    label="${gen_label[$i]}"
    scp -q "$target:$REMOTE_OUT/scaling-$label-summary.json" "$OUT/scaling-$label-summary.json" 2>/dev/null || true
    scp -q "$target:$REMOTE_OUT/scaling-$label.log" "$OUT/scaling-$label.log" 2>/dev/null || true
  fi
done

kubectl -n "$NS" get pods -l app=aggregator -o wide > "$OUT/scaling-pods.txt" 2>&1 || true
kubectl -n "$NS" describe deployment/aggregator > "$OUT/scaling-deployment.txt" 2>&1 || true
restore_aggregator_replicas
kubectl -n "$NS" rollout status deployment/aggregator --timeout=180s >/dev/null 2>&1 || true

summary_files=()
missing=0
for label in "${gen_label[@]}"; do
  file="$OUT/scaling-$label-summary.json"
  if [ -s "$file" ]; then
    summary_files+=("$file")
  else
    echo "[load] missing summary for $label: $file" >&2
    missing=1
  fi
done
if [ "$missing" -ne 0 ]; then
  echo "[load] FAIL: at least one generator did not produce a summary" >&2
  exit 1
fi

echo "[load] aggregating ${#summary_files[@]} generator summaries"
set +e
"$PYTHON" "$ROOT/loadtest/summarize.py" \
  --min-vus "$GATE_MIN_VUS" \
  --min-duration "$GATE_MIN_DURATION" \
  --duration "$DURATION_SECONDS" \
  --json-out "$OUT/scaling-aggregate.json" \
  "${summary_files[@]}"
aggregate_status=$?
set -e

detector_status=0
if [ "$RUN_DETECTOR" = "1" ]; then
  echo "[load] running exactly-once detector"
  set +e
  "$ROOT/loadtest/duplicate-detector.sh" 2>&1 | tee "$OUT/scaling-detector.txt"
  detector_status=${PIPESTATUS[0]}
  set -e
fi

if [ "$aggregate_status" -ne 0 ] || [ "$detector_status" -ne 0 ]; then
  echo "[load] RESULT: FAIL (aggregate=$aggregate_status detector=$detector_status)"
  exit 1
fi
echo "[load] RESULT: PASS"
