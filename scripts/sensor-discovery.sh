#!/usr/bin/env bash
# Probe the black-box Sensor Pantai image. Never modifies the image.
# Usage: ./scripts/sensor-discovery.sh [profile] [seed] [speed]
set -euo pipefail

PROFILE="${1:-practice}"
SEED="${2:-42}"
SPEED="${3:-60}"
TOKEN="${PANTAI_AUTH_TOKEN:-dev-pantai-token}"
IMAGE="sustuna/sensor-pantai@sha256:51fbd90d3891ad6e9f5768ad330ffb65205644cb29e50da65c7538f2e0b4397d"
NAME="pantai-discovery-$$"
PORT="${PORT:-18087}"
OUT="docs/sensor-discovery-${PROFILE}-${SEED}.log"

cleanup() { docker rm -f "$NAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo "image: $IMAGE" | tee "$OUT"
echo "profile=$PROFILE seed=$SEED speed=$SPEED" | tee -a "$OUT"
docker inspect "$IMAGE" > "docs/sensor-image-inspect.json" 2>/dev/null || true

docker run -d --name "$NAME" -p "${PORT}:8080" \
  -e PANTAI_PROFILE="$PROFILE" -e PANTAI_SEED="$SEED" -e PANTAI_SPEED="$SPEED" \
  -e PANTAI_AUTH_TOKEN="$TOKEN" "$IMAGE" >/dev/null

AUTH=(-H "Authorization: Bearer $TOKEN")
ready=0
for _ in $(seq 1 30); do
  if curl -fsS "http://127.0.0.1:${PORT}/healthz" >/dev/null 2>&1; then ready=1; break; fi
  sleep 1
done
if [ "$ready" -ne 1 ]; then echo "sensor did not become ready" | tee -a "$OUT"; exit 1; fi

curl -fsS "http://127.0.0.1:${PORT}/v1/stations" "${AUTH[@]}" > "docs/sensor-stations.json"
echo "stations: $(python3 -c 'import json;print(len(json.load(open("docs/sensor-stations.json"))["items"]))')" | tee -a "$OUT"

STATION="$(python3 -c 'import json;print(json.load(open("docs/sensor-stations.json"))["items"][0]["station_id"])')"
CURSOR=""
for i in $(seq 1 40); do
  TS="$(date -u +%FT%TZ)"
  HEALTH="$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:${PORT}/healthz")"
  URL="http://127.0.0.1:${PORT}/v1/observations?station_id=${STATION}&limit=100"
  if [ -n "$CURSOR" ]; then URL="${URL}&cursor=${CURSOR}"; fi
  BODY="$(curl -sS "${AUTH[@]}" "$URL")"
  echo "$BODY" > "docs/sensor-last-page.json"
  python3 - "$TS" "$HEALTH" "$BODY" <<'PY' | tee -a "$OUT"
import json, sys
ts, health, body = sys.argv[1], sys.argv[2], sys.argv[3]
try:
    page = json.loads(body)
except Exception:
    print(f"{ts} health={health} INVALID_JSON {body[:120]!r}")
    raise SystemExit
items = page.get("items") or []
observed = [i.get("observed_at") for i in items if isinstance(i, dict) and i.get("observed_at")]
observed.sort()
newest = observed[-1] if observed else None
ids = [i.get("observation_id") for i in items if isinstance(i, dict)]
print(f"{ts} health={health} generated_at={page.get('generated_at')} items={len(items)} newest_observed_at={newest} next_cursor={'yes' if page.get('next_cursor') else 'no'} sample_ids={ids[:3]}")
PY
  CURSOR="$(python3 -c 'import json;print(json.load(open("docs/sensor-last-page.json")).get("next_cursor") or "")' 2>/dev/null || true)"
  sleep 1
done

# Restart: /data is emptyDir, so this simulates the pod being rescheduled.
echo "--- restart (state reset) ---" | tee -a "$OUT"
docker restart "$NAME" >/dev/null
sleep 3
curl -sS "${AUTH[@]}" "http://127.0.0.1:${PORT}/v1/observations?station_id=${STATION}&limit=5&cursor=definitely-invalid" | head -c 500 | tee -a "$OUT" || true
echo | tee -a "$OUT"
echo "--- container logs (tail) ---" | tee -a "$OUT"
docker logs --tail 80 "$NAME" 2>&1 | tee -a "$OUT"

echo "wrote $OUT"
