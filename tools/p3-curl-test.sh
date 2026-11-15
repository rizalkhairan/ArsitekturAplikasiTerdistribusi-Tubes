#!/usr/bin/env bash
set -Eeuo pipefail

CLIENT_API_URL="${CLIENT_API_URL:-http://localhost:9080}"
INGEST_URL="${INGEST_URL:-http://localhost:8094}"
PVMBG_URL="${PVMBG_URL:-http://localhost:8082}"
BNPB_URL="${BNPB_URL:-http://localhost:8085}"
AUDIT_TOKEN="${AUDIT_TOKEN:-local-dev-audit-token}"
OPS_TOKEN="${OPS_TOKEN:-local-dev-ops-token}"
PIPELINE_INTERNAL_TOKEN="${PIPELINE_INTERNAL_TOKEN:-local-dev-pipeline-internal-token}"
FIELD_REPORT_TOKEN="${FIELD_REPORT_TOKEN:-local-dev-field-report-token}"
PVMBG_ADMIN_USER="${PVMBG_ADMIN_USER:-admin}"
PVMBG_ADMIN_PASSWORD="${PVMBG_ADMIN_PASSWORD:-local-dev-admin-password}"
FIELD_USER_PASSWORD="${FIELD_USER_PASSWORD:-field-pass-2026}"
MEDIA_USER_PASSWORD="${MEDIA_USER_PASSWORD:-media-pass-2026}"

tmp="$(mktemp -d)"
restore_bnpb=0
restore_consumer=0
cleanup() {
  if (( restore_bnpb )); then docker compose start bnpb >/dev/null || true; fi
  if (( restore_consumer )); then docker compose start field-report-consumer >/dev/null || true; fi
  curl -fsS --max-time 5 -u "$PVMBG_ADMIN_USER:$PVMBG_ADMIN_PASSWORD" \
    -X POST "$PVMBG_URL/admin/schema-version" -H 'Content-Type: application/json' \
    -d '{"mode":"baseline"}' >/dev/null || true
  rm -rf "$tmp"
}
trap cleanup EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }
pass() { echo "PASS: $*"; }
wait_for() {
  local description="$1"; shift
  for _ in $(seq 1 40); do
    if "$@"; then return 0; fi
    sleep 2
  done
  fail "timed out waiting for $description"
}

# Wrapper saves response body and asserts the expected HTTP status.
curl_status() {
  local expected="$1" output="$2"; shift 2
  local metrics actual elapsed
  metrics="$(curl -sS -o "$output" -w '%{http_code} %{time_total}' "$@")" || fail "curl failed: $*"
  actual="${metrics%% *}"
  elapsed="${metrics#* }"
  [[ "$actual" == "$expected" ]] || {
    cat "$output" >&2
    fail "expected HTTP $expected, received $actual ($*)"
  }
  if [[ "$expected" == 202 ]] && ! awk -v t="$elapsed" 'BEGIN { exit !(t <= 2.0) }'; then
    fail "202 response took ${elapsed}s; expected <= 2s"
  fi
}

echo 'P3.1 — health and access control'
curl_status 200 "$tmp/health" "$INGEST_URL/health"
curl_status 401 "$tmp/no-auth" "$INGEST_URL/internal/audit/unprocessed?source=PVMBG&limit=1"
curl_status 401 "$tmp/audit-raw" -H "Authorization: Bearer $AUDIT_TOKEN" \
  "$INGEST_URL/internal/ops/messages/not-a-real-message"
curl_status 401 "$tmp/internal-raw" -H "Authorization: Bearer $PIPELINE_INTERNAL_TOKEN" \
  "$INGEST_URL/internal/ops/messages/not-a-real-message"
curl_status 401 "$tmp/field-raw" -H "Authorization: Bearer $FIELD_REPORT_TOKEN" \
  "$INGEST_URL/internal/ops/messages/not-a-real-message"
curl_status 200 "$tmp/audit" -H "Authorization: Bearer $AUDIT_TOKEN" \
  "$INGEST_URL/internal/audit/unprocessed?source=PVMBG&limit=10"
if jq -e '.items | all(has("raw_base64") | not)' "$tmp/audit" >/dev/null; then
  pass 'Audit can list metadata, and does not receive raw payloads'
else
  fail 'Audit listing unexpectedly contains raw payload data'
fi

echo 'P3.2 — lossless quarantine for arbitrary raw bytes'
bad_utf8_b64="//4=" # bytes FF FE: not valid UTF-8
bad_utf8_id="p3-raw-$RANDOM"
curl_status 201 "$tmp/park" -X POST "$INGEST_URL/internal/dlq/park" \
  -H "Authorization: Bearer $PIPELINE_INTERNAL_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"message_id\":\"$bad_utf8_id\",\"source\":\"FIELD_REPORT\",\"reason\":\"invalid_utf8_demo\",\"retryable\":false,\"raw_base64\":\"$bad_utf8_b64\"}"
raw_id="$(jq -r .message_id "$tmp/park")"
curl_status 200 "$tmp/audit-raw-metadata" -H "Authorization: Bearer $AUDIT_TOKEN" \
  "$INGEST_URL/internal/audit/unprocessed?source=FIELD_REPORT&limit=1000"
jq -e --arg id "$raw_id" '.items[] | select(.message_id == $id and .source == "FIELD_REPORT" and .reason == "invalid_utf8_demo" and .received_at != "" and .retryable == false and .status == "parked")' \
  "$tmp/audit-raw-metadata" >/dev/null || fail 'Audit metadata is missing the raw failure source, reason, time, or retry class'
curl_status 200 "$tmp/raw" -H "Authorization: Bearer $OPS_TOKEN" \
  "$INGEST_URL/internal/ops/messages/$raw_id"
raw_returned_b64="$(jq -r .raw_base64 "$tmp/raw")"
[[ "$raw_returned_b64" == "$bad_utf8_b64" ]] \
  || fail 'raw bytes changed while being quarantined'
pass 'Ops retrieves the exact original invalid UTF-8 byte sequence'

echo 'P3.3 — retry unavailable canonical API, then recover'
docker compose stop bnpb
restore_bnpb=1
retry_ref="p3-retry-$RANDOM"
retry_json="{\"report_id\":\"$retry_ref\",\"volcano_id\":\"merapi\",\"alert_level\":\"WASPADA\",\"reported_at\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",\"eruption_count_24h\":1}"
retry_b64="$(printf '%s' "$retry_json" | base64 | tr -d '\n')"
curl_status 201 "$tmp/retry-park" -X POST "$INGEST_URL/internal/dlq/park" \
  -H "Authorization: Bearer $PIPELINE_INTERNAL_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"source\":\"PVMBG\",\"source_ref_id\":\"$retry_ref\",\"reason\":\"canonical_api_unavailable\",\"retryable\":true,\"raw_base64\":\"$retry_b64\"}"
retry_id="$(jq -r .message_id "$tmp/retry-park")"
retry_attempts() {
  curl -fsS -H "Authorization: Bearer $AUDIT_TOKEN" \
    "$INGEST_URL/internal/audit/unprocessed?source=PVMBG&limit=1000" >"$tmp/retrying"
  jq -r --arg id "$retry_id" '[.items[] | select(.message_id == $id and .status == "retrying" and .retryable == true)] | first | .attempts // 0' "$tmp/retrying"
}
for _ in $(seq 1 20); do
  attempts="$(retry_attempts)"
  [[ "$attempts" -ge 2 ]] && break
  sleep 1
done
[[ "${attempts:-0}" -ge 2 ]] || fail 'retryable message did not remain retrying with attempts increasing'
curl_status 200 "$tmp/retry-raw" -H "Authorization: Bearer $OPS_TOKEN" \
  "$INGEST_URL/internal/ops/messages/$retry_id"
jq -e '.source == "PVMBG" and .retryable == true and .reason == "canonical_api_unavailable" and .received_at != "" and .attempts >= 2 and .raw_base64 != ""' \
  "$tmp/retry-raw" >/dev/null || fail 'retryable raw message metadata is incomplete'
docker compose start bnpb
restore_bnpb=0
is_recovered() {
  curl -fsS -H "Authorization: Bearer $AUDIT_TOKEN" \
    "$INGEST_URL/internal/audit/unprocessed?source=PVMBG&limit=1000" \
    | jq -e --arg id "$retry_id" 'all(.items[]; .message_id != $id)' >/dev/null
}
wait_for 'retry recovery after BNPB restart' is_recovered
pass 'retrying record was replayed and removed from the unprocessed list'

echo 'P3.4 — PVMBG schema drift, raw inspection, and replay'
canonical_baseline_ready() {
  curl -fsS -H "X-Internal-Key: ${INTERNAL_API_KEY:-local-dev-internal-key}" \
    "$BNPB_URL/hazards?source=PVMBG&limit=1000" \
    | jq -e 'length >= 20' >/dev/null
}
wait_for 'PVMBG baseline seed reports to be consumed before the schema test' canonical_baseline_ready
# An additive field is accepted and retained in canonical attributes.
curl_status 200 "$tmp/schema" -u "$PVMBG_ADMIN_USER:$PVMBG_ADMIN_PASSWORD" \
  -X POST "$PVMBG_URL/admin/schema-version" -H 'Content-Type: application/json' \
  -d '{"mode":"additive"}'
additive_seen() {
  curl -fsS -H "X-Internal-Key: ${INTERNAL_API_KEY:-local-dev-internal-key}" \
    "$BNPB_URL/hazards?source=PVMBG&limit=100" \
    | jq -e 'any(.[]; .attributes.confidence_level != null)' >/dev/null
}
wait_for 'additive PVMBG confidence_level to reach canonical storage' additive_seen
pass 'additive PVMBG attribute is retained in canonical HazardEvent.attributes'
curl_status 200 "$tmp/schema" -u "$PVMBG_ADMIN_USER:$PVMBG_ADMIN_PASSWORD" \
  -X POST "$PVMBG_URL/admin/schema-version" -H 'Content-Type: application/json' \
  -d '{"mode":"baseline"}'

for mode in rename type_change missing_required; do
  curl -fsS -H "Authorization: Bearer $AUDIT_TOKEN" \
    "$INGEST_URL/internal/audit/unprocessed?source=PVMBG&limit=1000" \
    | jq '[.items[].message_id]' >"$tmp/schema-before"
  curl_status 200 "$tmp/schema" -u "$PVMBG_ADMIN_USER:$PVMBG_ADMIN_PASSWORD" \
    -X POST "$PVMBG_URL/admin/schema-version" -H 'Content-Type: application/json' \
    -d "{\"mode\":\"$mode\"}"
  found=''
  for _ in $(seq 1 35); do
    curl -fsS -H "Authorization: Bearer $AUDIT_TOKEN" \
      "$INGEST_URL/internal/audit/unprocessed?source=PVMBG&limit=1000" >"$tmp/items"
    found="$(jq -r --slurpfile before "$tmp/schema-before" \
      '[.items[] | select(.reason | startswith("schema_incompatible:"))
       | select(.message_id as $id | ($before[0] | index($id)) == null)]
       | last | .message_id // empty' "$tmp/items")"
    if [[ -n "$found" ]]; then break; fi
    sleep 2
  done
  [[ -n "$found" ]] || fail "PVMBG $mode did not create a schema incompatibility quarantine"
  # Stop the incompatible mock mode at the first newly quarantined report so
  # the following source records do not accumulate extra failed entries.
  curl_status 200 "$tmp/schema" -u "$PVMBG_ADMIN_USER:$PVMBG_ADMIN_PASSWORD" \
    -X POST "$PVMBG_URL/admin/schema-version" -H 'Content-Type: application/json' \
    -d '{"mode":"baseline"}'
  # Ops-only route returns raw_base64; replay uses the same message_id.
  curl_status 200 "$tmp/detail" -H "Authorization: Bearer $OPS_TOKEN" \
    "$INGEST_URL/internal/ops/messages/$found"
  parked_attempts="$(jq -r .attempts "$tmp/detail")"
  sleep 3
  curl_status 200 "$tmp/detail-after-wait" -H "Authorization: Bearer $OPS_TOKEN" \
    "$INGEST_URL/internal/ops/messages/$found"
  jq -e --argjson attempts "$parked_attempts" '.status == "parked" and .retryable == false and .attempts == $attempts' \
    "$tmp/detail-after-wait" >/dev/null \
    || fail "PVMBG $mode permanent schema failure was retried automatically"
  cp "$tmp/detail-after-wait" "$tmp/detail"
  jq -e '.raw_base64 | type == "string" and length > 0' "$tmp/detail" >/dev/null \
    || fail "Ops raw inspection returned no original PVMBG payload for $mode"
  jq -e '.source == "PVMBG" and .retryable == false and .status == "parked" and .received_at != "" and (.reason | startswith("schema_incompatible:"))' "$tmp/detail" >/dev/null \
    || fail "PVMBG $mode failure was not durably classified as a permanent schema incompatibility"
  curl_status 200 "$tmp/replay" -X POST "$INGEST_URL/internal/ops/reprocess" \
    -H "Authorization: Bearer $OPS_TOKEN" -H 'Content-Type: application/json' \
    -d "{\"message_id\":\"$found\"}"
  jq -e '.status == "recovered"' "$tmp/replay" >/dev/null \
    || fail "replay did not recover PVMBG $mode record"
  curl_status 200 "$tmp/detail-recovered" -H "Authorization: Bearer $OPS_TOKEN" \
    "$INGEST_URL/internal/ops/messages/$found"
  jq -e '.status == "recovered"' "$tmp/detail-recovered" >/dev/null \
    || fail "PVMBG $mode message is not durably marked recovered"
  report_raw="$(jq -r .raw_base64 "$tmp/detail-recovered")"
  canonical_ref="$(printf '%s' "$report_raw" | base64 -d | jq -r '.report_id // empty')"
  if [[ -z "$canonical_ref" ]]; then
    canonical_ref="pvmbg-v2-$(printf '%s' "$report_raw" | base64 -d | sha256sum | awk '{print $1}')"
  fi
  curl_status 200 "$tmp/replayed-hazards" -H "X-Internal-Key: ${INTERNAL_API_KEY:-local-dev-internal-key}" \
    "$BNPB_URL/hazards?source=PVMBG&limit=1000"
  jq -e --arg id "$canonical_ref" '[.[] | select(.source_ref_id == $id)] | length == 1' \
    "$tmp/replayed-hazards" >/dev/null \
    || fail "PVMBG $mode replay did not produce exactly one canonical event"
  pass "$mode incompatibility was quarantined, inspectable by Ops, and replayed"
  sleep 11 # wait for a fresh PVMBG report before testing the next incompatible mode
done
curl_status 200 "$tmp/schema" -u "$PVMBG_ADMIN_USER:$PVMBG_ADMIN_PASSWORD" \
  -X POST "$PVMBG_URL/admin/schema-version" -H 'Content-Type: application/json' \
  -d '{"mode":"baseline"}'

# Two permanently failed messages for one source_ref_id cannot be replayed out
# of order. Once both are repaired, canonical storage still has one source row.
echo 'P3.4b — per-source_ref_id replay order and deduplication'
order_ref="p3-order-$RANDOM"
order_early="p3-order-early-$RANDOM"
order_late="p3-order-late-$RANDOM"
order_time="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
raw_early="{\"report_id\":\"$order_ref\",\"volcano_id\":\"merapi\",\"alert_level\":\"WASPADA\",\"reported_at\":\"$order_time\",\"confidence_level\":0.91}"
raw_late="{\"report_id\":\"$order_ref\",\"volcano_id\":\"merapi\",\"alert_level\":\"SIAGA\",\"reported_at\":\"$order_time\",\"confidence_level\":0.95}"
early_b64="$(printf '%s' "$raw_early" | base64 | tr -d '\n')"
late_b64="$(printf '%s' "$raw_late" | base64 | tr -d '\n')"
curl_status 201 "$tmp/order-early-park" -X POST "$INGEST_URL/internal/dlq/park" \
  -H "Authorization: Bearer $PIPELINE_INTERNAL_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"message_id\":\"$order_early\",\"source\":\"PVMBG\",\"source_ref_id\":\"$order_ref\",\"received_at\":\"2026-10-01T00:00:01Z\",\"reason\":\"schema_incompatible:test\",\"retryable\":false,\"raw_base64\":\"$early_b64\"}"
curl_status 201 "$tmp/order-late-park" -X POST "$INGEST_URL/internal/dlq/park" \
  -H "Authorization: Bearer $PIPELINE_INTERNAL_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"message_id\":\"$order_late\",\"source\":\"PVMBG\",\"source_ref_id\":\"$order_ref\",\"received_at\":\"2026-10-01T00:00:02Z\",\"reason\":\"schema_incompatible:test\",\"retryable\":false,\"raw_base64\":\"$late_b64\"}"
curl_status 409 "$tmp/order-out-of-order" -X POST "$INGEST_URL/internal/ops/reprocess" \
  -H "Authorization: Bearer $OPS_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"message_id\":\"$order_late\"}"
curl_status 200 "$tmp/order-first" -X POST "$INGEST_URL/internal/ops/reprocess" \
  -H "Authorization: Bearer $OPS_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"message_id\":\"$order_early\"}"
curl_status 200 "$tmp/order-second" -X POST "$INGEST_URL/internal/ops/reprocess" \
  -H "Authorization: Bearer $OPS_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"message_id\":\"$order_late\"}"
curl_status 200 "$tmp/order-hazards" -H "X-Internal-Key: ${INTERNAL_API_KEY:-local-dev-internal-key}" \
  "$BNPB_URL/hazards?source=PVMBG&limit=1000"
jq -e --arg id "$order_ref" '[.[] | select(.source_ref_id == $id)] | length == 1' "$tmp/order-hazards" >/dev/null \
  || fail 'ordered replay created a duplicate canonical row for the same source_ref_id'
for order_id in "$order_early" "$order_late"; do
  curl_status 200 "$tmp/order-recovered" -H "Authorization: Bearer $OPS_TOKEN" \
    "$INGEST_URL/internal/ops/messages/$order_id"
  jq -e '.status == "recovered"' "$tmp/order-recovered" >/dev/null \
    || fail "ordered message $order_id was not marked recovered"
done
curl_status 200 "$tmp/order-replay-again" -X POST "$INGEST_URL/internal/ops/reprocess" \
  -H "Authorization: Bearer $OPS_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"message_id\":\"$order_early\"}"
jq -e '.status == "recovered" and .replayed == true' "$tmp/order-replay-again" >/dev/null \
  || fail 'replaying an already recovered message was not idempotent'
curl_status 200 "$tmp/order-hazards-again" -H "X-Internal-Key: ${INTERNAL_API_KEY:-local-dev-internal-key}" \
  "$BNPB_URL/hazards?source=PVMBG&limit=1000"
jq -e --arg id "$order_ref" '[.[] | select(.source_ref_id == $id)] | length == 1' "$tmp/order-hazards-again" >/dev/null \
  || fail 'replaying an already recovered message created a duplicate canonical row'
pass 'out-of-order replay is rejected; ordered replay recovers both messages into one canonical source row'

echo 'P3.5 — field report scope, idempotency, and poison-record recovery'
login() {
  local username="$1" password="$2" output="$3"
  curl_status 200 "$output" -X POST "$CLIENT_API_URL/auth/login" \
    -H 'Content-Type: application/json' -d "{\"username\":\"$username\",\"password\":\"$password\"}"
}
login field-user "$FIELD_USER_PASSWORD" "$tmp/field-login"
field_token="$(jq -r '.data.accessToken // .accessToken // empty' "$tmp/field-login")"
[[ -n "$field_token" ]] || fail 'field-user login response did not include accessToken'
login media-user "$MEDIA_USER_PASSWORD" "$tmp/media-login"
media_token="$(jq -r '.data.accessToken // .accessToken // empty' "$tmp/media-login")"
[[ -n "$media_token" ]] || fail 'media-user login response did not include accessToken'

curl_status 403 "$tmp/media-write" -X POST "$CLIENT_API_URL/field-reports" \
  -H "Authorization: Bearer $media_token" -H 'Idempotency-Key: p3-media-forbidden' \
  -H 'Content-Type: application/json' -d '{"description":"must be forbidden"}'
pass 'media user cannot write a field report'

docker compose stop field-report-consumer
restore_consumer=1
key="p3-field-$RANDOM"
report='{"report_id":"p3-field-report","reported_at":"2026-11-15T00:00:00Z","description":"P3 end-to-end test report"}'
curl_status 202 "$tmp/accepted-1" -X POST "$CLIENT_API_URL/field-reports" \
  -H "Authorization: Bearer $field_token" -H "Idempotency-Key: $key" \
  -H 'Content-Type: application/json' -d "$report"
jq -e '.status == "accepted" and .durability == "broker-acknowledged" and .receipt_id != ""' "$tmp/accepted-1" >/dev/null \
  || fail '202 response did not contain the accepted receipt and broker acknowledgement'
curl_status 202 "$tmp/accepted-2" -X POST "$CLIENT_API_URL/field-reports" \
  -H "Authorization: Bearer $field_token" -H "Idempotency-Key: $key" \
  -H 'Content-Type: application/json' -d "$report"
receipt1="$(jq -r .receipt_id "$tmp/accepted-1")"
receipt2="$(jq -r .receipt_id "$tmp/accepted-2")"
[[ "$receipt1" == "$receipt2" ]] || fail 'same idempotency key/body returned different receipts'
curl_status 409 "$tmp/conflict" -X POST "$CLIENT_API_URL/field-reports" \
  -H "Authorization: Bearer $field_token" -H "Idempotency-Key: $key" \
  -H 'Content-Type: application/json' -d '{"description":"different content"}'

# More acknowledgements while the consumer is stopped must remain in Kafka.
backlog_receipts=()
for n in 1 2 3; do
  backlog_key="p3-backlog-$RANDOM-$n"
  backlog_report="{\"report_id\":\"$backlog_key\",\"reported_at\":\"2026-11-15T00:00:00Z\",\"description\":\"durability backlog $n\"}"
  curl_status 202 "$tmp/backlog-$n" -X POST "$CLIENT_API_URL/field-reports" \
    -H "Authorization: Bearer $field_token" -H "Idempotency-Key: $backlog_key" \
    -H 'Content-Type: application/json' -d "$backlog_report"
  backlog_receipts+=("$(jq -r .receipt_id "$tmp/backlog-$n")")
done

docker compose start field-report-consumer
restore_consumer=0
processed_once() {
  local receipt success
  for receipt in "$receipt1" "${backlog_receipts[@]}"; do
    success="$(docker compose logs --no-color field-report-consumer 2>/dev/null \
      | grep -F "receipt_id=$receipt" | grep -c 'result=success' || true)"
    [[ "$success" == 1 ]] || return 1
  done
  return 0
}
wait_for 'all 202-acknowledged field reports to be processed once' processed_once

# Kill immediately after a fresh 202 acknowledgement, then restart. Kafka retains
# the record; the consumer's SQLite idempotency key prevents a second side effect.
crash_key="p3-crash-$RANDOM"
crash_report="{\"report_id\":\"$crash_key\",\"reported_at\":\"2026-11-15T00:00:00Z\",\"description\":\"consumer crash window test\"}"
curl_status 202 "$tmp/crash-accepted" -X POST "$CLIENT_API_URL/field-reports" \
  -H "Authorization: Bearer $field_token" -H "Idempotency-Key: $crash_key" \
  -H 'Content-Type: application/json' -d "$crash_report"
crash_receipt="$(jq -r .receipt_id "$tmp/crash-accepted")"
restore_consumer=1
docker compose kill -s SIGKILL field-report-consumer
docker compose start field-report-consumer
restore_consumer=0
crash_processed_once() {
  local success
  success="$(docker compose logs --no-color field-report-consumer 2>/dev/null \
    | grep -F "receipt_id=$crash_receipt" | grep -c 'result=success' || true)"
  [[ "$success" == 1 ]]
}
wait_for 'report acknowledged before consumer SIGKILL to be processed once' crash_processed_once

curl_status 400 "$tmp/invalid-json" -X POST "$CLIENT_API_URL/field-reports" \
  -H "Authorization: Bearer $field_token" -H "Idempotency-Key: p3-invalid-json" \
  -H 'Content-Type: application/json' -d '{"description":'
curl_status 200 "$tmp/ingest-health" "$INGEST_URL/health"
pass '202 returns within 2s while consumer is stopped; queued reports survive downtime/SIGKILL and each has one successful logical insertion; invalid JSON is rejected'

echo 'P3.6 — poison Kafka records are quarantined without stopping consumer'
login field-user "$FIELD_USER_PASSWORD" "$tmp/field-login-final"
field_token="$(jq -r '.data.accessToken // .accessToken // empty' "$tmp/field-login-final")"
before="$(curl -fsS -H "Authorization: Bearer $AUDIT_TOKEN" \
  "$INGEST_URL/internal/audit/unprocessed?source=FIELD_REPORT&limit=1000" \
  | jq '[.items[].message_id]')"
declare -A poison_restart_counts
for service in kafka ingest-pipeline field-report-consumer bnpb client-api auth pvmbg; do
  container="$(docker compose ps -q "$service")"
  [[ -n "$container" ]] || fail "$service container is missing before poison test"
  poison_restart_counts[$service]="$(docker inspect -f '{{.RestartCount}}' "$container")"
done
cat >"$tmp/p3-poison-producer.go" <<'GO'
package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	if len(os.Args) != 3 { log.Fatal("usage: p3-poison-producer <broker> <nonce>") }
	client, err := kgo.NewClient(kgo.SeedBrokers(os.Args[1]), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil { log.Fatal(err) }
	defer client.Close()
	nonce := os.Args[2]
	truncated := []byte(fmt.Sprintf("{\"broken\":\"%s", nonce))
	deep := []byte(strings.Repeat("[", 70) + fmt.Sprintf("\"%s\"", nonce) + strings.Repeat("]", 70))
	oversized := append(bytes.Repeat([]byte("x"), 262145), []byte(nonce)...)
	invalidUTF8 := append([]byte{0xff, 0xfe}, []byte(nonce)...)
	values := [][]byte{truncated, deep, oversized, invalidUTF8}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for i, value := range values {
		record := &kgo.Record{Topic: "field-reports", Key: []byte(fmt.Sprintf("p3-poison-%d", i)), Value: value}
		if err := client.ProduceSync(ctx, record).FirstErr(); err != nil { log.Fatal(err) }
	}
}
GO
broker_arch="$(docker image inspect apache/kafka:3.9.0 --format '{{.Architecture}}')"
case "$broker_arch" in
  amd64|arm64) ;;
  *) fail "unsupported Kafka image architecture for Go helper: $broker_arch" ;;
esac
(
  cd services/ingest-pipeline
  CGO_ENABLED=0 GOOS=linux GOARCH="$broker_arch" go build -trimpath \
    -o "$tmp/p3-poison-producer" "$tmp/p3-poison-producer.go"
)
docker compose cp "$tmp/p3-poison-producer" kafka:/tmp/p3-poison-producer
poison_nonce="p3-$RANDOM-$(date +%s)"
MSYS_NO_PATHCONV=1 docker compose exec -u 0 -T kafka /bin/sh -c \
  "chmod +x /tmp/p3-poison-producer && /tmp/p3-poison-producer kafka:9092 $poison_nonce"
poison_parked() {
  curl -fsS -H "Authorization: Bearer $AUDIT_TOKEN" \
    "$INGEST_URL/internal/audit/unprocessed?source=FIELD_REPORT&limit=1000" \
    | jq -e --argjson old "$before" '[.items[] | select(.message_id as $id | ($old | index($id)) == null)] | length >= 4' >/dev/null
}
wait_for 'truncated, deep, oversized, and invalid UTF-8 cases to be quarantined' poison_parked
curl -fsS -H "Authorization: Bearer $AUDIT_TOKEN" \
  "$INGEST_URL/internal/audit/unprocessed?source=FIELD_REPORT&limit=1000" >"$tmp/poison-items"
jq --argjson old "$before" '[.items[] | select(.message_id as $id | ($old | index($id)) == null)]' \
  "$tmp/poison-items" >"$tmp/poison-new"
jq -e '
  any(.[]; .reason == "malformed_json_or_invalid_utf8")
  and any(.[]; .reason == "json_nesting_exceeds_64_levels")
  and any(.[]; .reason | startswith("payload_too_large:"))
  and all(.[]; .source == "FIELD_REPORT" and .received_at != "" and .retryable == false and .status == "parked")' \
  "$tmp/poison-new" >/dev/null \
  || fail 'one or more malformed input classes did not retain the expected quarantine reason'

# Inspect every poison payload through the Ops-only path and compare exact bytes.
deep_expected=""
for _ in $(seq 1 70); do deep_expected+="["; done
deep_expected+="\"$poison_nonce\""
for _ in $(seq 1 70); do deep_expected+="]"; done
truncated_payload="{\"broken\":\"$poison_nonce"
truncated_b64="$(printf '%s' "$truncated_payload" | base64 | tr -d '\n')"
deep_b64="$(printf '%s' "$deep_expected" | base64 | tr -d '\n')"
oversize_b64="$( { head -c 262145 /dev/zero | tr '\000' x; printf '%s' "$poison_nonce"; } | base64 | tr -d '\n')"
invalid_utf8_b64="$(printf '\377\376%s' "$poison_nonce" | base64 | tr -d '\n')"
truncated_count=0
deep_count=0
oversize_count=0
utf8_count=0
while IFS= read -r poison_id; do
  poison_id="${poison_id%$'\r'}" # Windows jq output may use CRLF.
  [[ -n "$poison_id" ]] || continue
  curl_status 200 "$tmp/poison-detail" -H "Authorization: Bearer $OPS_TOKEN" \
    "$INGEST_URL/internal/ops/messages/$poison_id"
  raw="$(jq -r .raw_base64 "$tmp/poison-detail")"
  case "$raw" in
    "$invalid_utf8_b64") utf8_count=$((utf8_count + 1)) ;;
    "$truncated_b64") truncated_count=$((truncated_count + 1)) ;;
    "$deep_b64") deep_count=$((deep_count + 1)) ;;
    "$oversize_b64") oversize_count=$((oversize_count + 1)) ;;
    *) fail "Ops raw bytes did not match a sent poison record ($poison_id)" ;;
  esac
done < <(jq -r '.[].message_id' "$tmp/poison-new")
[[ "$utf8_count" == 1 && "$truncated_count" == 1 && "$deep_count" == 1 && "$oversize_count" == 1 ]] \
  || fail 'expected one exact raw DLQ payload for each of the four poison classes'

# A clean field report sent after poison records still completes successfully.
control_key="p3-after-poison-$RANDOM"
control_report="{\"report_id\":\"$control_key\",\"reported_at\":\"2026-11-15T00:00:00Z\",\"description\":\"valid report after poison records\"}"
curl_status 202 "$tmp/after-poison-accepted" -X POST "$CLIENT_API_URL/field-reports" \
  -H "Authorization: Bearer $field_token" -H "Idempotency-Key: $control_key" \
  -H 'Content-Type: application/json' -d "$control_report"
control_receipt="$(jq -r .receipt_id "$tmp/after-poison-accepted")"
control_processed() {
  [[ "$(docker compose logs --no-color field-report-consumer 2>/dev/null \
    | grep -F "receipt_id=$control_receipt" | grep -c 'result=success' || true)" == 1 ]]
}
wait_for 'valid report after poison records to be processed once' control_processed

for service in kafka ingest-pipeline field-report-consumer bnpb client-api auth pvmbg; do
  container="$(docker compose ps -q "$service")"
  [[ -n "$container" ]] || fail "$service container is missing"
  [[ "$(docker inspect -f '{{.State.Running}}' "$container")" == true ]] \
    || fail "$service is not running after malformed messages"
  [[ "$(docker inspect -f '{{.RestartCount}}' "$container")" == "${poison_restart_counts[$service]}" ]] \
    || fail "$service restarted after malformed messages"
done
pass 'all four malformed classes are quarantined, service containers remain running, and a later valid report succeeds once'

echo 'All P3 checks passed.'

