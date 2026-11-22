#!/usr/bin/env bash
set -Eeuo pipefail
NS="${NAMESPACE:-sistem-informasi-kebencanaan-bnpb}"
OUT="${OUT:?Set OUT}"
mkdir -p "$OUT"
kubectl get nodes -o json >"$OUT/nodes.json"
jq -e '[.items[] | select(any(.status.conditions[]; .type=="Ready" and .status=="True"))] | length>=2' "$OUT/nodes.json" >/dev/null || { echo 'BLOCKED: at least two Ready nodes required'; exit 2; }
kubectl -n "$NS" get deployments,statefulsets -o json | jq '[.items[] | {kind, name:.metadata.name,containers:[.spec.template.spec.containers[]|{name,resources}],volumes:.spec.template.spec.volumes}]' >"$OUT/resources-and-volumes.json"
jq -e 'all(.[]; all(.containers[]; .resources.requests.memory != null and .resources.limits.memory != null))' "$OUT/resources-and-volumes.json" >/dev/null || { echo 'FAIL: missing memory requests/limits'; exit 1; }
kubectl -n "$NS" get networkpolicies -o json >"$OUT/networkpolicies.json"
kubectl -n "$NS" get pvc -o json >"$OUT/pvcs.json"
kubectl -n tester-agent get daemonsets -o json >"$OUT/tester-agent.json"
jq -e 'any(.items[]; .metadata.name=="tester-agent" and .status.desiredNumberScheduled>=2 and .status.numberReady==.status.desiredNumberScheduled)' "$OUT/tester-agent.json" >/dev/null || { echo 'BLOCKED: Tester Agent is not ready across nodes'; exit 2; }
cat >"$OUT/manual-verification.md" <<'TEXT'
# Platform checks requiring independent evidence

The runner saved node readiness, memory declarations, PVCs, policies and Tester
Agent scheduling. These do not prove the following. Attach evidence separately:

- Separate VM/native kernels and physical-host topology; not kind/k3d nodes.
- Exact resource class values versus Lampiran B (including applicable CPU rules).
- Secret/ConfigMap use and runtime negative connection tests for storage isolation.
- Original pinned Tester Agent image and official scenario logs/auditor results.
- Metrics dashboard for each service, cross-service trace, correlation-ID logs.
- Required video demonstration, DB view, report explanations and AI form.

No full Ketentuan Implementasi compliance claim is made by these smoke checks.
TEXT
echo "Platform smoke checks passed; review $OUT/manual-verification.md before acceptance."
