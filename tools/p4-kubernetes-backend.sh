# Sourced by p4-curl-test.sh; uses the real deployed canonical API credentials.
NS="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
RUN="p4-check-$(date +%s)-$RANDOM"
OUT="${OUT:?Set OUT for Kubernetes evidence}"
mkdir -p "$OUT"
k() { kubectl -n "$NS" "$@"; }
forward_pids=()
cleanup_kube() {
  local rc=$?
  k logs "deployment/$RUN" >"$OUT/processor.log" 2>&1 || true
  for pid in "${forward_pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  k delete deployment,service,configmap,pvc -l "acceptance-run=$RUN" --wait=false >/dev/null || true
  rm -rf "$tmp"
  return "$rc"
}
trap cleanup_kube EXIT
# Copy the deployed processor's image, resources and canonical credentials.
# Use a fresh PVC and source fixture; never reset the production processor DB.
k get deployment satellite-processor -o json >"$tmp/deployed.json"
jq --arg run "$RUN" '
  .metadata={name:$run,labels:{"acceptance-run":$run}} |
  del(.status) | .spec.replicas=1 | .spec.selector.matchLabels={app:$run} |
  .spec.template.metadata={labels:{app:$run,"acceptance-run":$run,role:"broker"}} |
  .spec.template.spec.securityContext.fsGroup=65532 |
  .spec.template.spec.volumes |= map(if has("persistentVolumeClaim") then .persistentVolumeClaim.claimName=$run else . end) |
  .spec.template.spec.containers[0].env |= (map(select(.name != "SATELLITE_BASE_URL" and .name != "SATELLITE_TOKEN" and .name != "P4_TEST_MODE" and .name != "P4_TEST_TOKEN")) +
    [{name:"SATELLITE_BASE_URL",value:("http://"+$run+"-mock:8098")},{name:"SATELLITE_TOKEN",value:"local-satellite-test-token"},{name:"P4_TEST_MODE",value:"true"},{name:"P4_TEST_TOKEN",value:"local-p4-test-token"}])
' "$tmp/deployed.json" >"$tmp/processor.json"
k create configmap "$RUN-mock" --from-file=server.py=tools/satellite-mock/server.py --dry-run=client -o json |
  jq --arg run "$RUN" '.metadata.labels={"acceptance-run":$run}' | k apply -f -
cat >"$tmp/resources.yaml" <<YAML
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: $RUN
  labels: {acceptance-run: $RUN}
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: local-path
  resources: {requests: {storage: 2Gi}}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: $RUN-mock
  labels: {acceptance-run: $RUN}
spec:
  replicas: 1
  selector: {matchLabels: {app: $RUN-mock}}
  template:
    metadata: {labels: {app: $RUN-mock, acceptance-run: $RUN}}
    spec:
      containers:
      - name: mock
        image: python:3.12-alpine
        command: [python, /fixture/server.py]
        resources:
          requests: {cpu: 100m, memory: 64Mi}
          limits: {cpu: 500m, memory: 128Mi}
        volumeMounts: [{name: fixture, mountPath: /fixture, readOnly: true}]
      volumes: [{name: fixture, configMap: {name: $RUN-mock}}]
---
apiVersion: v1
kind: Service
metadata:
  name: $RUN-mock
  labels: {acceptance-run: $RUN}
spec:
  selector: {app: $RUN-mock}
  ports: [{port: 8098, targetPort: 8098}]
---
apiVersion: v1
kind: Service
metadata:
  name: $RUN
  labels: {acceptance-run: $RUN}
spec:
  selector: {app: $RUN}
  ports: [{port: 8080, targetPort: 8080}]
YAML
k apply -f "$tmp/resources.yaml"
k apply -f "$tmp/processor.json"
k rollout status "deployment/$RUN-mock" --timeout=180s
k rollout status "deployment/$RUN" --timeout=180s
for spec in "$RUN:8096:8080" "$RUN-mock:8098:8098"; do
  IFS=: read -r service port target <<<"$spec"
  NAMESPACE="$NS" bash scripts/port-forward-loop.sh "$service" "$port" "$target" >"$OUT/$service-forward.log" 2>&1 &
  forward_pids+=("$!")
done
compose() {
  case "$1" in
    up) k rollout status "deployment/$RUN" --timeout=180s ;;
    kill)
      k logs "deployment/$RUN" >"$OUT/before-crash.log"
      pod="$(k get pods -l "app=$RUN" -o jsonpath='{.items[0].metadata.name}')"
      # Production image is distroless: do not exec a nonexistent shell.
      k delete pod "$pod" --grace-period=0 --force --wait=true >/dev/null
      restarted=0
      for _ in $(seq 1 60); do
        current="$(k get pods -l "app=$RUN" -o json | jq -r '.items[] | select(.metadata.deletionTimestamp == null) | .metadata.name')"
        if [[ -n "$current" && "$current" != "$pod" ]]; then restarted=1; break; fi
        sleep 1
      done
      [[ "$restarted" == 1 ]] || fail 'P4.5 force-deleted processor pod was not replaced'
      k rollout status "deployment/$RUN" --timeout=180s ;;
    *) fail 'Unsupported Kubernetes fixture operation' ;;
  esac
}
p4_full_recovery_check() {
  # Independent mapping examples at both count and FRP severity boundaries.
  jq -cn --arg n "$nonce" '{events:([{tag:"LOW",count:2,frp:1,lon:116},{tag:"COUNT6",count:6,frp:1,lon:117},{tag:"COUNT20",count:20,frp:1,lon:118},{tag:"FRP30",count:2,frp:15,lon:119},{tag:"FRP100",count:2,frp:50,lon:121},{tag:"CHAIN",count:3,frp:1,lon:122}] | map(. as $g | range(0;.count) as $i | {type:"detection",data:{satellite:($g.tag+"-"+$n+"-"+($i|tostring)),acq_date:"2026-11-17",acq_time:(if $i==0 then "1000" else "1010" end),latitude:-2,longitude:($g.lon+(if $g.tag=="CHAIN" then $i*0.008 else $i*0.00001 end)),confidence:"h",frp:$g.frp}}))}' >"$tmp/boundaries.json"
  boundary_result="$(post_events "$tmp/boundaries.json")"
  resume_offset="$(jq -r .latest_offset <<<"$boundary_result")"
  wait_processed "$resume_offset"
  get_state >"$OUT/mapping-state.json"
  for expectation in LOW:WASPADA:2 COUNT6:SIAGA:6 COUNT20:AWAS:20 FRP30:SIAGA:2 FRP100:AWAS:2 CHAIN:WASPADA:3; do
    IFS=: read -r tag severity count <<<"$expectation"
    jq -e --arg prefix "$tag-$nonce-" --arg severity "$severity" --argjson count "$count" '[.fires[] | select(any(.attributes.detection_keys[]; startswith($prefix)))] | length==1 and .[0].severity==$severity and .[0].attributes.detection_count==$count and .[0].latitude == -2 and .[0].occurred_at == "2026-11-17T10:00:00Z"' "$OUT/mapping-state.json" >/dev/null || fail "P4.1 mapping boundary $tag failed"
  done
  jq -e --arg prefix "CHAIN-$nonce-" '[.fires[] | select(any(.attributes.detection_keys[]; startswith($prefix)))][0] | ((.longitude-122.008)|fabs)<0.000001 and (.attributes.satellites|length)==3' "$OUT/mapping-state.json" >/dev/null || fail 'P4.1 chain centroid/satellite set mismatch'
  pass 'P4.1 count/FRP severity thresholds, transitive chain, centroid, earliest event time'
  # A second, isolated processor replays the same source without any reset of
  # the running recovered instance or writes to already-versioned canonical rows.
  get_state | jq -S '.fires | sort_by(.source_ref_id)' >"$OUT/recovered.json"
  get_state | jq '[.mutations[].source_ref_id] | unique' >"$OUT/synthetic-refs.json"
  get_status >"$OUT/controlled-status.json"
  k scale "deployment/$RUN" --replicas=0 >/dev/null
  k wait --for=delete pod -l "app=$RUN" --timeout=180s >/dev/null
  jq --arg name "$RUN-baseline" '.metadata.name=$name | .spec.selector.matchLabels={app:$name} | .spec.template.metadata.labels.app=$name |
    .spec.template.spec.volumes |= map(if has("persistentVolumeClaim") then del(.persistentVolumeClaim) | .emptyDir={} else . end) |
    .spec.template.spec.containers[0].env |= map(if .name=="CANONICAL_API_URL" then {name:.name,value:"http://127.0.0.1:1"} else . end)' "$tmp/processor.json" | k apply -f -
  k rollout status "deployment/$RUN-baseline" --timeout=180s >/dev/null
  k port-forward "deployment/$RUN-baseline" 8099:8080 >"$OUT/baseline-forward.log" 2>&1 &
  forward_pids+=("$!")
  wait_for 'uninterrupted replay processed all source offsets' "curl -fsS http://localhost:8099/status | jq -e --argjson end '$resume_offset' '.processed_offset >= \$end' >/dev/null"
  curl -fsS -H "Authorization: Bearer $TEST_TOKEN" http://localhost:8099/__test/state | jq -S '.fires | sort_by(.source_ref_id)' >"$OUT/uninterrupted.json"
  diff -u "$OUT/uninterrupted.json" "$OUT/recovered.json" >"$OUT/recovery.diff" || fail 'P4.5 full recovered result differs from replay without interruption'
  pass 'P4.5 full fire projection matches uninterrupted replay'
  echo 'Baseline compares local business projection only; recovered run separately required canonical outbox delivery. Baseline uses disposable state and a non-routable sink.' >"$OUT/recovery-scope.txt"
}
