#!/usr/bin/env bash
# One entry point for the currently implemented Kubernetes P3/P4 checks.
set -Eeuo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
if [[ "${1:-}" == --help ]]; then
  echo 'Usage: bash tools/p3-p4-kubernetes-test.sh [--check-only]'
  echo 'Configuration: tools/.env.p3-p4 (private Bash exports), or environment variables.'
  echo 'Required: deployed cluster; P4_REFERENCE_FILE; P4_LOAD_SCRIPT; running live feed.'
  echo '--check-only performs non-disruptive prerequisite/readiness checks only.'
  exit 0
fi
[[ $# == 0 || ( $# == 1 && "$1" == --check-only ) ]] || { echo 'Unknown argument; use --help'; exit 2; }
if [[ "${P3_P4_CAPTURE_ACTIVE:-0}" != 1 ]]; then
  export EVIDENCE_ROOT="${EVIDENCE_ROOT:-$ROOT/test-results/p3-p4-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
  mkdir -p "$EVIDENCE_ROOT"
  # Capture before prerequisite checks, including missing kubeconfig/resources.
  started="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  set +e
  P3_P4_CAPTURE_ACTIVE=1 bash "$ROOT/tools/p3-p4-kubernetes-test.sh" "$@" 2>&1 | tee "$EVIDENCE_ROOT/console.log"
  statuses=("${PIPESTATUS[@]}")
  set -e
  result="${statuses[0]}"
  if [[ "${statuses[1]}" != 0 ]]; then result=1; fi
  case "$result" in
    0) label=PASS ;;
    2) label=BLOCKED ;;
    *) label=FAILED ;;
  esac
  {
    printf '# P3/P4 test run\n\n'
    printf 'Status: **%s**\n\nExit code: %s\n\n' "$label" "$result"
    printf 'Mode: %s\n\n' "${1:-full}"
    printf 'Started (UTC): %s\n\nFinished (UTC): %s\n\n' "$started" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'Full output: [console.log](console.log)\n\n'
    printf 'P3 output: [p3-console.log](p3-console.log), if reached.\n\n'
    printf 'P4 output: [p4-console.log](p4-console.log), if reached.\n\n'
    printf 'Evidence subfolders: `p3/` and `p4/`, if reached.\n\n'
    printf 'A failed prerequisite means later tests were not executed. A PASS records script assertions, not independent verification of platform compliance.\n'
  } >"$EVIDENCE_ROOT/summary.md"
  mkdir -p "$ROOT/test-results"
  printf '%s\n' "$EVIDENCE_ROOT" >"$ROOT/test-results/latest-run.txt"
  printf '\nRun status: %s (exit %s)\nSaved output: %s\n' "$label" "$result" "$EVIDENCE_ROOT"
  exit "$result"
fi
if [[ -f "$ROOT/tools/.env.p3-p4" ]]; then
  # This is a user-owned shell configuration, never a generated test artifact.
  source "$ROOT/tools/.env.p3-p4"
fi
for tool in kubectl curl jq go sha256sum; do
  command -v "$tool" >/dev/null || { echo "Missing prerequisite: $tool" >&2; exit 1; }
done
kubectl config current-context >/dev/null || { echo 'BLOCKED: configure KUBECONFIG or select a kubectl context'; exit 2; }
kubectl get nodes --request-timeout=15s >/dev/null || { echo 'BLOCKED: cluster is unreachable'; exit 2; }
EVIDENCE_ROOT="${EVIDENCE_ROOT:?Capture wrapper must provide evidence directory}"
mkdir -p "$EVIDENCE_ROOT"
if [[ "${1:-}" == --check-only ]]; then
  OUT="$EVIDENCE_ROOT/readiness" bash tools/p3-p4-readiness.sh
  echo 'Readiness checks completed; acceptance tests were NOT run.'
  exit 0
fi
for file in "${P4_REFERENCE_FILE:-}" "${P4_LOAD_SCRIPT:-}"; do
  [[ -n "$file" && -f "$file" ]] || { echo 'BLOCKED: configure P4_REFERENCE_FILE and P4_LOAD_SCRIPT before the full acceptance run'; exit 2; }
done
echo "Evidence: $EVIDENCE_ROOT"
OUT="$EVIDENCE_ROOT/readiness" bash tools/p3-p4-readiness.sh
echo 'Running P3 Kubernetes checks (temporarily interrupts application services).'
OUT="$EVIDENCE_ROOT/p3" bash tools/p3-kubernetes-test.sh 2>&1 | tee "$EVIDENCE_ROOT/p3-console.log"
echo 'Running P4 live checks; the satellite feed must be running.'
OUT="$EVIDENCE_ROOT/p4" bash tools/p4-kubernetes-test.sh 2>&1 | tee "$EVIDENCE_ROOT/p4-console.log"
echo 'PASS: P3/P4 Kubernetes assertions completed; inspect saved evidence and platform readiness separately.'
echo "Evidence: $EVIDENCE_ROOT"
