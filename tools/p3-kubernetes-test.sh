#!/usr/bin/env bash
# Run the shared P3 assertions against Kubernetes. No Docker daemon is used.
set -Eeuo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
export NAMESPACE="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
export OUT="${OUT:-$ROOT/test-results/p3-k8s-$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "$OUT"
for tool in kubectl curl jq go sha256sum; do command -v "$tool" >/dev/null || { echo "Missing $tool" >&2; exit 1; }; done
echo "Context: $(kubectl config current-context); namespace: $NAMESPACE"
declare -A original
pids=()
k() { kubectl -n "$NAMESPACE" "$@"; }
deployment() { if [[ "$1" == bnpb ]]; then echo aggregator; else echo "$1"; fi; }
pods() { k get pods -l "app=$(deployment "$1")" -o json; }
archive() {
  local service="$1" pod
  while read -r pod; do
    [[ -n "$pod" ]] || continue
    k logs "$pod" >"$OUT/$pod.log" 2>/dev/null || true
    k logs "$pod" --previous >"$OUT/$pod-previous.log" 2>/dev/null || true
  done < <(pods "$service" | jq -r '.items[].metadata.name')
}
restore_all() {
  local service
  for service in "${!original[@]}"; do k scale "deployment/$service" --replicas="${original[$service]}" >/dev/null || true; done
  for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  k get pods -o wide >"$OUT/pods-after.txt" 2>&1 || true
}
trap restore_all EXIT
for service in aggregator ingest-pipeline field-report-consumer client-api auth pvmbg kafka; do
  original[$service]="$(k get deployment "$service" -o jsonpath='{.spec.replicas}')"
  [[ "${original[$service]}" -gt 0 ]] || { echo "$service must be running" >&2; exit 1; }
  k rollout status "deployment/$service" --timeout=180s
done
k get pods -o wide >"$OUT/pods-before.txt"
# Load only named application credentials; never print the Secret.
secret_json="$(k get secret bnpb-secrets -o json)"
for pair in AUDIT_TOKEN:AUDIT_TOKEN OPS_TOKEN:OPS_TOKEN PIPELINE_INTERNAL_TOKEN:PIPELINE_INTERNAL_TOKEN FIELD_REPORT_TOKEN:FIELD_REPORT_TOKEN INTERNAL_API_KEY:INTERNAL_API_KEY PVMBG_ADMIN_USER:PVMBG_ADMIN_USERNAME PVMBG_ADMIN_PASSWORD:PVMBG_ADMIN_PASSWORD; do
  var="${pair%%:*}"; key="${pair#*:}"
  if [[ -z "${!var:-}" ]]; then
    value="$(jq -r --arg key "$key" '.data[$key] // empty' <<<"$secret_json" | base64 -d)"
    [[ -n "$value" ]] || { echo "Missing Secret key $key" >&2; exit 1; }
    export "$var=$value"
  fi
done
unset secret_json value
for spec in client-api:9080:8080 ingest-pipeline:8094:8094 pvmbg:8082:8082 aggregator:8085:8085; do
  IFS=: read -r service local_port remote_port <<<"$spec"
  bash scripts/port-forward-loop.sh "$service" "$local_port" "$remote_port" >"$OUT/forward-$service.log" 2>&1 &
  pids+=("$!")
done
for url in http://localhost:8094/health http://localhost:9080/health; do
  ready=0
  for _ in $(seq 1 60); do if curl -fsS --max-time 2 "$url" >/dev/null; then ready=1; break; fi; sleep 1; done
  [[ "$ready" == 1 ]] || { echo "Unreachable $url; check port conflicts" >&2; exit 1; }
done
# Narrow compatibility adapter for operations used by the shared Compose suite.
# Unsupported commands fail rather than falling back to a local Docker daemon.
docker() {
  local operation service pod format arch
  if [[ "$1 $2" == 'image inspect' ]]; then
    arch="$(k get nodes -o json | jq -r '[.items[].status.nodeInfo.architecture] | unique | if length == 1 then .[0] else error("mixed architectures require a dedicated producer image") end')"
    echo "$arch"; return
  fi
  if [[ "$1" == inspect ]]; then
    format="$3"; service="$4"
    if [[ "$format" == *RestartCount* ]]; then
      pods "$service" | jq -c '[.items[] | {uid:.metadata.uid,restarts:[.status.containerStatuses[]?.restartCount]}] | sort_by(.uid)'
    else
      pods "$service" | jq -r '(.items|length)>0 and all(.items[]; .status.phase == "Running" and all(.status.containerStatuses[]; .ready))'
    fi
    return
  fi
  [[ "$1" == compose ]] || { echo "Unsupported runtime command" >&2; return 1; }
  shift; operation="$1"; shift
  case "$operation" in
    stop)
      service="$(deployment "$1")"; archive "$1"
      k scale "deployment/$service" --replicas=0
      k wait --for=delete pod -l "app=$service" --timeout=180s ;;
    start)
      service="$(deployment "$1")"
      k scale "deployment/$service" --replicas="${original[$service]}"
      k rollout status "deployment/$service" --timeout=180s ;;
    kill)
      service="${@: -1}"; archive "$service"
      pod="$(pods "$service" | jq -r '.items[0].metadata.name')"
      old_restart="$(pods "$service" | jq '[.items[].status.containerStatuses[]?.restartCount] | add // 0')"
      # Application images must contain /bin/sh and kill. A failure is not a pass.
      k exec "$pod" -- /bin/sh -c 'kill -9 1' || {
        # Exec may return 137 because the target was killed. Verify the restart below.
        echo 'Kill exec closed; verifying restart' >&2
      }
      restarted=0
      for _ in $(seq 1 60); do
        if [[ "$(pods "$service" | jq '[.items[].status.containerStatuses[]?.restartCount] | add // 0')" -gt "$old_restart" ]]; then restarted=1; break; fi
        sleep 1
      done
      [[ "$restarted" == 1 ]] || { echo 'No verified container restart' >&2; return 1; }
      k rollout status "deployment/$(deployment "$service")" --timeout=180s ;;
    logs)
      service="${@: -1}"
      # Refresh current pod logs, retain logs from replaced pods exactly once.
      archive "$service"
      find "$OUT" -maxdepth 1 -name "$(deployment "$service")-*.log" -type f -exec cat {} + ;;
    ps) echo "${@: -1}" ;;
    cp)
      pod="$(pods kafka | jq -r '.items[0].metadata.name')"
      k cp "$1" "$pod:${2#kafka:}" ;;
    exec)
      while [[ "$1" != kafka ]]; do shift; done; shift
      pod="$(pods kafka | jq -r '.items[0].metadata.name')"
      k exec "$pod" -- "$@" ;;
    *) echo "Unsupported Kubernetes runtime operation: $operation" >&2; return 1 ;;
  esac
}
p3_receiver_crash() {
  echo 'P3.6 requirement: killing receiving ingest API after acknowledged backlog, before consumer resumes'
  docker compose kill -s SIGKILL ingest-pipeline
  # The Service forward reconnects automatically; retry the same accepted body/key.
  for _ in $(seq 1 60); do
    if curl -fsS --max-time 2 "$INGEST_URL/health" >/dev/null; then break; fi
    sleep 1
  done
  curl_status 202 "$tmp/receiver-retry" -X POST "$CLIENT_API_URL/field-reports" \
    -H "Authorization: Bearer $field_token" -H "Idempotency-Key: $key" \
    -H 'Content-Type: application/json' -d "$report"
  [[ "$(jq -r .receipt_id "$tmp/receiver-retry")" == "$receipt1" ]] || fail 'receiver crash changed the durable receipt'
}
# Sourcing preserves the runtime functions and saved replica counts.
p3_verify_durable_receipts() {
  cat >"$tmp/verify-receipts.go" <<'GO'
package main
import("database/sql";"fmt";"os";_ "modernc.org/sqlite")
func main(){
 db,err:=sql.Open("sqlite","file:/app/data/ingest.db?mode=ro");if err!=nil{panic(err)};defer db.Close()
 for _,receipt:=range os.Args[1:]{var n int;err=db.QueryRow("SELECT COUNT(*) FROM processed_field_reports WHERE receipt_id=? AND length(payload)>0 AND length(payload_hash)>0",receipt).Scan(&n);if err!=nil{panic(err)};if n!=1{fmt.Fprintf(os.Stderr,"receipt %s has %d durable results\n",receipt,n);os.Exit(1)};fmt.Printf("PASS durable receipt=%s rows=%d\n",receipt,n)}
}
GO
  arch="$(pods field-report-consumer | jq -r '.items[0].spec.nodeName' | xargs kubectl get node -o jsonpath='{.status.nodeInfo.architecture}')"
  (cd services/ingest-pipeline; CGO_ENABLED=0 GOOS=linux GOARCH="$arch" go build -o "$tmp/verify-receipts" "$tmp/verify-receipts.go")
  pod="$(pods field-report-consumer | jq -r '.items[0].metadata.name')"
  k cp "$tmp/verify-receipts" "$pod:/tmp/p3-verify-receipts"
  k exec "$pod" -- /bin/sh -c 'chmod +x /tmp/p3-verify-receipts'
  k exec "$pod" -- /tmp/p3-verify-receipts "$receipt1" "${backlog_receipts[@]}" "$crash_receipt" "$control_receipt" | tee "$OUT/durable-receipts.txt"
}
# The shared script installs its own cleanup; wrap it to also restore Kubernetes.
source tools/p3-curl-test.sh
