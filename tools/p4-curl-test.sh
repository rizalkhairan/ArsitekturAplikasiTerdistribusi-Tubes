#!/usr/bin/env bash
# M2 Problem 4 acceptance checks:
# 1. Hotspot -> WILDFIRE mapping; one fire across multiple satellites.
# 2. Event-time grouping, declared lateness, and counted late arrivals.
# 3. Updates/retractions preserve fire identity and version order.
# 4. 60x-style burst catch-up <=60s, p95 latency, dashboard backlog visibility.
# 5. SIGKILL during in-flight work resumes without loss or duplication.
# 6. Reference comparison (requires P4_REFERENCE_FILE; otherwise BLOCKED).
# 7. Live feed (requires --live with a token or --live-external against the deployment).
# 8. Reconnect resumes from the correct offset.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
MODE="${1:-local}"
for tool in curl jq; do command -v "$tool" >/dev/null || { echo "Missing required tool: $tool" >&2; exit 1; }; done
if [[ "$MODE" != "--live-external" && "$MODE" != "live-external" && "${P4_BACKEND:-compose}" != kubernetes ]]; then
  command -v docker >/dev/null || { echo "Missing required tool: docker (use --live-external to observe a deployed processor without Docker)" >&2; exit 1; }
fi
LOCAL_URL="http://localhost:8096"
MOCK_URL="http://localhost:8098"
TEST_TOKEN="local-p4-test-token"
MOCK_TOKEN="local-satellite-test-token"
if [[ -z "${P4_FEED_TOKEN:-}" && -f "$ROOT/.env" ]]; then
  while IFS= read -r line || [[ -n "$line" ]]; do
    if [[ "$line" == P4_FEED_TOKEN=* ]]; then P4_FEED_TOKEN="${line#P4_FEED_TOKEN=}"; fi
  done <"$ROOT/.env"
fi
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }
pass() { echo "PASS: $*"; }
skip() { echo "SKIP: $*"; }
compose() { docker compose --profile p4-test "$@"; }
if [[ "${P4_BACKEND:-compose}" == kubernetes ]]; then
  source tools/p4-kubernetes-backend.sh
fi
get_status() { curl -fsS "$LOCAL_URL/status"; }
get_state() { curl -fsS -H "Authorization: Bearer $TEST_TOKEN" "$LOCAL_URL/__test/state"; }
status_field() { get_status | jq -r "$1"; }
wait_for() {
  local label="$1" predicate="$2" n
  for n in $(seq 1 240); do
    if eval "$predicate"; then return 0; fi
    sleep 0.5
  done
  fail "timed out waiting for $label; status=$(get_status 2>/dev/null || true)"
}
wait_processed() {
  local target="$1" n current
  for n in $(seq 1 240); do
    current="$(get_status 2>/dev/null || true)"
    if jq -e --argjson target "$target" '.processed_offset >= $target and .pending_canonical_mutations == 0' >/dev/null 2>&1 <<<"$current"; then return 0; fi
    sleep 0.5
  done
  fail "timed out at offset $target; status=$(get_status 2>/dev/null || true)"
}
post_events() { curl -fsS -X POST "$MOCK_URL/__admin/events" -H 'Content-Type: application/json' --data-binary "@$1"; }

if [[ "$MODE" == "--live" || "$MODE" == "live" || "$MODE" == "--live-external" || "$MODE" == "live-external" ]]; then
  external=0
  if [[ "$MODE" == "--live-external" || "$MODE" == "live-external" ]]; then external=1; fi
  export P4_LIVE_SECONDS="${P4_LIVE_SECONDS:-180}"

  if [ "$external" = 1 ]; then
    LOCAL_URL="${P4_LIVE_URL:-http://localhost:8096}"
    echo "Observing the deployed satellite processor at $LOCAL_URL (Docker not required)."
  else
    [[ -n "${P4_FEED_TOKEN:-}" ]] || fail 'Set P4_FEED_TOKEN before live mode. The script never starts an exam session.'
    [[ -n "${P4_CANONICAL_URL:-}" && -n "${P4_CANONICAL_TOKEN:-}" ]] || fail 'Set P4_CANONICAL_URL and P4_CANONICAL_TOKEN to the deployed A/B canonical API before live mode'
    export P4_FEED_URL="${P4_FEED_URL:-https://satellite.naufarrel.dev}"
    export P4_START_OFFSET="${P4_START_OFFSET:-0}"
    export P4_DB_FILE="p4-live-$RANDOM-$(date +%s).db"
    export P4_PROVINCE_FILE="${P4_PROVINCE_FILE:-$ROOT/services/satellite-processor/testdata/province-centroids.json}"
    [[ -f "$P4_PROVINCE_FILE" ]] || fail 'Set P4_PROVINCE_FILE to the BNPB province-reference JSON for live processing'
    session="$(curl -fsS -H "Authorization: Bearer $P4_FEED_TOKEN" "$P4_FEED_URL/v1/session")" || fail 'Could not read the satellite session; start the exam session first'
    speed="$(jq -r '.speed // empty' <<<"$session")"
    if [[ "${P4_REQUIRE_60X:-0}" == "1" ]]; then
      [[ "$speed" == 60 || "$speed" == 60x || "$speed" == '60×' ]] || fail "Live session must be at 60x speed (reported: $speed)"
    else
      echo "P4.7 live session speed=${speed:-unknown}; set P4_REQUIRE_60X=1 only when the live session must be 60x."
    fi
    echo "Starting the processor against $P4_FEED_URL for $P4_LIVE_SECONDS seconds."
    compose up -d --build --force-recreate canonical-contract-stub satellite-mock satellite-processor
  fi
  wait_for 'processor health' "curl -fsS '$LOCAL_URL/health' >/dev/null 2>&1"
  before="$(status_field .source_offset)"
  sleep "$P4_LIVE_SECONDS"
  live="$(get_status)"
  latest="$(jq -r .latest_offset <<<"$live")"; source="$(jq -r .source_offset <<<"$live")"; processed="$(jq -r .processed_offset <<<"$live")"
  [[ "$latest" -gt "$before" ]] || fail "live feed did not advance from offset $before"
  [[ "$source" -gt "$before" && "$processed" -gt "$before" ]] || fail "live input was not durably processed: before=$before source=$source processed=$processed latest=$latest"
  [[ "$(jq -r .feed_connected <<<"$live")" == true ]] || fail "feed disconnected during live run: $(jq -r .last_error <<<"$live")"
  pass "P4.7 live feed processed; durable=$source processed=$processed upstream_latest=$latest ingest_lag=$(jq -r .ingest_lag <<<"$live") processing_lag=$(jq -r .processing_lag <<<"$live") pending_writes=$(jq -r .pending_canonical_mutations <<<"$live") orphan_updates=$(jq -r .orphan_updates <<<"$live") p95=$(jq -r .p95_input_to_store_seconds <<<"$live")s"
  if [ "$external" = 1 ]; then
    skip 'P4.6 external live mode has no /__test/state; run local mode with P4_REFERENCE_FILE for the reference comparison'
  elif [[ -n "${P4_REFERENCE_FILE:-}" && -f "$P4_REFERENCE_FILE" ]]; then
    get_state | jq -S '[.fires[] | {source_ref_id, hazard_type:"WILDFIRE", severity, area_name, latitude, longitude, occurred_at, attributes}] | sort_by(.source_ref_id)' >"$tmp/actual.json"
    jq -S '[.fires[] | {source_ref_id, hazard_type:"WILDFIRE", severity, area_name, latitude, longitude, occurred_at, attributes}] | sort_by(.source_ref_id)' "$P4_REFERENCE_FILE" >"$tmp/expected.json"
    diff -u "$tmp/expected.json" "$tmp/actual.json" || fail 'P4.6 differs from supplied reference file'
    pass 'P4.6 full active-fire output matches the supplied reference file'
  else skip 'P4.6 requires the assistant reference result in P4_REFERENCE_FILE'; fi
  pass "P4.8 live SSE connection remained active; reconnect attempts=$(jq -r .reconnect_attempts <<<"$live")"
  exit 0
fi
[[ "$MODE" == "local" || "$MODE" == "--local" ]] || fail 'Usage: tools/p4-curl-test.sh [local|--live|--live-external]'
unset P4_FEED_URL P4_FEED_TOKEN
export P4_FEED_TOKEN="$MOCK_TOKEN"
export P4_DB_FILE="p4-local-$RANDOM-$(date +%s).db"
export P4_START_OFFSET=0
compose up -d --build --force-recreate canonical-contract-stub satellite-mock
wait_for 'local mock feed' "curl -fsS -H 'Authorization: Bearer $MOCK_TOKEN' '$MOCK_URL/v1/session' >/dev/null 2>&1"
curl -fsS -X POST "$MOCK_URL/__admin/reset" -H 'Content-Type: application/json' -d '{}' >/dev/null
compose up -d --build --force-recreate satellite-processor
wait_for 'processor health' "curl -fsS '$LOCAL_URL/health' >/dev/null 2>&1"
curl -fsS -X POST "$LOCAL_URL/__test/reset" -H "Authorization: Bearer $TEST_TOKEN" -H 'Content-Type: application/json' -d '{}' >/dev/null

nonce="p4-$RANDOM-$(date +%s)"
cat >"$tmp/initial.json" <<JSON
{"events":[
 {"type":"detection","data":{"satellite":"N20-$nonce","acq_date":"2026-11-12","acq_time":"0200","latitude":-2.0,"longitude":113.0,"confidence":"h","frp":8.0}},
 {"type":"detection","data":{"satellite":"N21-$nonce","acq_date":"2026-11-12","acq_time":"0210","latitude":-2.001,"longitude":113.001,"confidence":"n","frp":9.0}},
 {"type":"detection","data":{"satellite":"N22-$nonce","acq_date":"2026-11-12","acq_time":"0211","latitude":-2.0,"longitude":113.0001,"confidence":"low","frp":80.0}},
 {"type":"detection","data":{"satellite":"SINGLE-$nonce","acq_date":"2026-11-12","acq_time":"0230","latitude":-8.0,"longitude":120.0,"confidence":"h","frp":1.0}}
]}
JSON
first="$(post_events "$tmp/initial.json")"; offset="$(jq -r .latest_offset <<<"$first")"; wait_processed "$offset"
initial="$(get_status)"
[[ "$(jq -r .active_fires <<<"$initial")" == 1 ]] || fail 'P4.1 expected one fire from two close detections across satellites'
[[ "$(jq -r .singleton_detections <<<"$initial")" == 1 ]] || fail 'P4.1 singleton was not counted'
fire="$(get_state | jq -c --arg n "$nonce" '[.fires[] | select(any(.attributes.detection_keys[]; contains($n)))] | first')"
[[ "$fire" != null ]] || fail 'P4.1 wildfire projection is missing'
fire_ref="$(get_state | jq -r '.mutations[0].source_ref_id')"
jq -e '.hazard_type == "WILDFIRE" and .area_name == "Kalimantan Tengah" and .attributes.detection_count == 2 and .attributes.max_confidence == "h" and .attributes.total_frp_mw == 17' <<<"$fire" >/dev/null || fail 'P4.1 mapped fields are wrong'
pass 'P4.1 exact-key dedupe, confidence filter, cross-satellite cluster, singleton count, wildfire mapping'

cat >"$tmp/duplicate.json" <<JSON
{"events":[{"type":"detection","data":{"satellite":"N20-$nonce","acq_date":"2026-11-12","acq_time":"0200","latitude":-2.0,"longitude":113.0,"confidence":"h","frp":8.0}}]}
JSON
dup="$(post_events "$tmp/duplicate.json")"; offset="$(jq -r .latest_offset <<<"$dup")"; wait_processed "$offset"
[[ "$(status_field .active_detections)" == 4 ]] || fail 'P4.1 duplicate input was counted twice'

cat >"$tmp/late.json" <<JSON
{"events":[
 {"type":"detection","data":{"satellite":"FUTURE-$nonce","acq_date":"2026-11-12","acq_time":"1230","latitude":-8.0,"longitude":120.0,"confidence":"h","frp":1.0}},
 {"type":"detection","data":{"satellite":"LATE-$nonce","acq_date":"2026-11-12","acq_time":"0205","latitude":-2.0005,"longitude":113.0005,"confidence":"h","frp":2.0}}
]}
JSON
late_before="$(status_field .late_detections)"
late="$(post_events "$tmp/late.json")"; offset="$(jq -r .latest_offset <<<"$late")"; wait_processed "$offset"
late_status="$(get_status)"; late_after_beyond="$(jq -r .late_detections <<<"$late_status")"
[[ "$late_after_beyond" -gt "$late_before" ]] || fail 'P4.2 beyond-lateness detection was not counted'
late_fire="$(get_state | jq -c --arg n "$nonce" '[.fires[] | select(any(.attributes.detection_keys[]; contains($n)))] | first')"
jq -e '.attributes.detection_count == 3' <<<"$late_fire" >/dev/null || fail 'P4.2 beyond-lateness detection was silently dropped'

# max_event_time is now 12:30, so the watermark is 06:30. A 07:50
# detection is behind max_event_time but inside the 6h allowed lateness: it
# must update the fire without incrementing late_detections.
within_key="WITHIN-$nonce|2026-11-12|0750|-2.0005|113.0005"
cat >"$tmp/within.json" <<JSON
{"events":[{"type":"detection","data":{"satellite":"WITHIN-$nonce","acq_date":"2026-11-12","acq_time":"0750","latitude":-2.0005,"longitude":113.0005,"confidence":"h","frp":3.0}}]}
JSON
within="$(post_events "$tmp/within.json")"; offset="$(jq -r .latest_offset <<<"$within")"; wait_processed "$offset"
within_status="$(get_status)"; within_late="$(jq -r .late_detections <<<"$within_status")"
[[ "$within_late" -eq "$late_after_beyond" ]] || fail 'P4.2 within-lateness detection incorrectly incremented late_detections'
within_fire="$(get_state | jq -c --arg n "$nonce" '[.fires[] | select(any(.attributes.detection_keys[]; contains($n)))] | first')"
jq -e '.attributes.detection_count == 4' <<<"$within_fire" >/dev/null || fail 'P4.2 within-lateness detection did not update the fire'
pass 'P4.2 event-time grouping; within-lateness updates; beyond-lateness retained and counted'

key_b="N21-$nonce|2026-11-12|0210|-2.001|113.001"
cat >"$tmp/update.json" <<JSON
{"events":[{"type":"update","data":{"target_key":"$key_b","detection":{"satellite":"N21-$nonce","acq_date":"2026-11-12","acq_time":"0210","latitude":-2.001,"longitude":113.001,"confidence":"h","frp":18.0}}}]}
JSON
updated="$(post_events "$tmp/update.json")"; updated_offset="$(jq -r .latest_offset <<<"$updated")"; wait_processed "$updated_offset"
cat >"$tmp/retract.json" <<JSON
{"events":[
 {"type":"retract","data":{"detection_key":"$key_b"}},
 {"type":"retract","data":{"detection_key":"LATE-$nonce|2026-11-12|0205|-2.0005|113.0005"}},
 {"type":"retract","data":{"detection_key":"$within_key"}}
]}
JSON
correction="$(post_events "$tmp/retract.json")"; offset="$(jq -r .latest_offset <<<"$correction")"; wait_processed "$offset"
mutations="$(get_state | jq -c --arg ref "$fire_ref" '[.mutations[] | select(.source_ref_id == $ref)]')"
jq -e '. as $m | ($m|length) >= 4 and $m[-1].operation == "retract" and all($m[:-1][]; .operation == "upsert") and ([$m[].source_ref_id] | unique | length == 1) and ([$m[].version] == [range(1; ($m|length)+1)])' <<<"$mutations" >/dev/null || fail 'P4.3 update/retract did not keep lineage and version order'
[[ "$(status_field .active_fires)" == 0 ]] || fail 'P4.3 fire remained after all but one detection were retracted'
pass 'P4.3 update, shrinking fire, and disappearance use versioned mutations on one fire identity'

jq -cn --arg nonce "$nonce" '{events:[range(0;200) as $i | range(0;2) as $side | {type:"detection",data:{satellite:("BURST"+($side|tostring)+"-"+$nonce+"-"+($i|tostring)),acq_date:"2026-11-14",acq_time:(if $side==0 then "1000" else "1010" end),latitude:(-1.5+($i%20)*0.12),longitude:(112.5+(($i/20|floor)*0.12)+($side*0.0002)),confidence:"h",frp:0.5}}]}' >"$tmp/burst.json"
started="$(date +%s)"; burst="$(post_events "$tmp/burst.json")"; offset="$(jq -r .latest_offset <<<"$burst")"
max_lag=0
for _ in $(seq 1 40); do
  lag="$(status_field .processing_lag 2>/dev/null || echo 0)"
  [[ "$lag" =~ ^[0-9]+$ ]] || lag=0
  if (( lag > max_lag )); then max_lag="$lag"; fi
  sleep 0.1
done
wait_processed "$offset"; elapsed=$(( $(date +%s) - started ))
[[ "$elapsed" -le 60 ]] || fail "P4.4 catch-up exceeded 60s (${elapsed}s)"
burst_status="$(get_status)"
[[ "$(jq -r .processing_lag <<<"$burst_status")" == 0 ]] || fail 'P4.4 processing lag remains'
jq -e '.p95_input_to_store_seconds | numbers' <<<"$burst_status" >/dev/null || fail 'P4.4 p95 input-to-store latency was not reported'
curl -fsS "$LOCAL_URL/dashboard" >"$tmp/p4-dashboard.html"
grep -q 'Processing lag' "$tmp/p4-dashboard.html" || fail 'P4.4 dashboard does not expose processing lag'
grep -q 'p95 receipt to store' "$tmp/p4-dashboard.html" || fail 'P4.4 dashboard does not expose p95 latency'
burst_fires="$(get_state | jq -c --arg n "$nonce" '[.fires[] | select(any(.attributes.detection_keys[]; contains($n) and startswith("BURST")))]')"
jq -e 'length == 200 and (map(.attributes.detection_count) | add) == 400' <<<"$burst_fires" >/dev/null || fail 'P4.4 burst offsets did not produce all 200 unique fire clusters'
[[ "$max_lag" -gt 0 ]] || echo 'NOTE: P4.4 processing_lag was not sampled above zero; the dashboard field and final catch-up were verified' >&2
pass "P4.4 400-event 60x-style burst caught up in ${elapsed}s; max_sampled_backlog=${max_lag}; p95=$(jq -r .p95_input_to_store_seconds <<<"$burst_status")s"

crash_nonce="crash-$nonce"
cat >"$tmp/crash-a.json" <<JSON
{"events":[{"type":"detection","data":{"satellite":"CRASHA-$crash_nonce","acq_date":"2026-11-15","acq_time":"0300","latitude":-2.2,"longitude":113.2,"confidence":"h","frp":4}}]}
JSON
crash_a="$(post_events "$tmp/crash-a.json")"; crash_offset="$(jq -r .latest_offset <<<"$crash_a")"
wait_for 'crash input durably received' "[[ \$(status_field .source_offset) -ge $crash_offset ]]"
crash_processed="$(status_field .processed_offset)"
echo "P4.5 kill after durable offset=$crash_offset processed=$crash_processed" >&2
compose kill -s SIGKILL satellite-processor >/dev/null
cat >"$tmp/crash-b.json" <<JSON
{"events":[{"type":"detection","data":{"satellite":"CRASHB-$crash_nonce","acq_date":"2026-11-15","acq_time":"0305","latitude":-2.2005,"longitude":113.2005,"confidence":"h","frp":5}}]}
JSON
crash_b="$(post_events "$tmp/crash-b.json")"; crash_final="$(jq -r .latest_offset <<<"$crash_b")"; compose up -d satellite-processor >/dev/null; wait_processed "$crash_final"
crash_fires="$(get_state | jq -c --arg n "$crash_nonce" '[.fires[] | select(any(.attributes.detection_keys[]; contains($n)))]')"
jq -e 'length == 1 and .[0].attributes.detection_count == 2' <<<"$crash_fires" >/dev/null || fail 'P4.5 restart lost or duplicated the in-progress fire'
pass 'P4.5 SIGKILL/restart resumes from durable offset and produces one final fire'

before_reconnect="$(status_field .reconnect_attempts)"; before_offset="$(status_field .source_offset)"
before_resume_detections="$(status_field .active_detections)"
curl -fsS -X POST "$MOCK_URL/__admin/disconnect" -H 'Content-Type: application/json' -d '{}' >/dev/null
cat >"$tmp/resume.json" <<JSON
{"events":[{"type":"heartbeat","data":{"virtual_now":"2026-11-16T00:00:00Z"}},{"type":"detection","data":{"satellite":"RESUME-$nonce","acq_date":"2026-11-15","acq_time":"0500","latitude":-7.0,"longitude":125.0,"confidence":"h","frp":1}}]}
JSON
resume="$(post_events "$tmp/resume.json")"; resume_offset="$(jq -r .latest_offset <<<"$resume")"; wait_processed "$resume_offset"
wait_for 'feed reconnection' "[[ \$(status_field .reconnect_attempts) -gt $before_reconnect ]]"
[[ "$(status_field .source_offset)" == "$resume_offset" ]] || fail 'P4.8 source offset is not at the latest feed offset'
[[ "$resume_offset" -gt "$before_offset" ]] || fail 'P4.8 no data consumed after reconnect'
[[ "$(status_field .active_detections)" -eq $((before_resume_detections + 1)) ]] || fail 'P4.8 reconnect lost or duplicated detection effects'
pass 'P4.8 forced disconnect resumes contiguously and reaches latest offset'
if declare -F p4_full_recovery_check >/dev/null; then p4_full_recovery_check; fi

if [[ -n "${P4_REFERENCE_FILE:-}" && -f "$P4_REFERENCE_FILE" ]]; then
  get_state | jq -S '[.fires[] | {source_ref_id, hazard_type:"WILDFIRE", severity, area_name, latitude, longitude, occurred_at, attributes}] | sort_by(.source_ref_id)' >"$tmp/actual.json"
  jq -S '[.fires[] | {source_ref_id, hazard_type:"WILDFIRE", severity, area_name, latitude, longitude, occurred_at, attributes}] | sort_by(.source_ref_id)' "$P4_REFERENCE_FILE" >"$tmp/expected.json"
  diff -u "$tmp/expected.json" "$tmp/actual.json" || fail 'P4.6 differs from the supplied reference file'
  pass 'P4.6 local stream matches supplied reference file'
else skip 'P4.6 BLOCKED: assistant comparison requires P4_REFERENCE_FILE from the exam/reference result'; fi
if [[ -n "${P4_FEED_TOKEN:-}" ]]; then
  skip 'P4.7 BLOCKED here: run --live (starting a local processor) or --live-external (observing the deployed processor)'
else
  skip 'P4.7 BLOCKED here: run --live-external against the deployed processor, or set P4_FEED_TOKEN for --live'
fi
echo 'P4 local checks completed; P4.6/P4.7 require the external reference/session or --live-external.'
