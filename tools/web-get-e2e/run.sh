#!/usr/bin/env bash
# GET-carrier browser fixture: telemt test harness <- GET-only nginx <-
# real Chromium (Playwright 1.62.0). Run from WSL:
#   tools/web-get-e2e/run.sh https          # Https carrier
#   tools/web-get-e2e/run.sh https-lanes    # HttpsLanes carrier
# The telemt fixture and nginx container are left running for inspection;
# the fixture self-terminates after TELEMT_WEB_E2E_TTL_SECS (default 600).
set -euo pipefail
cd "$(dirname "$0")"

CARRIER="${1:-https}"
PORT="${TELEMT_WEB_E2E_PORT:-18081}"
PARENT_PORT=18000
CONTAINER=telemt-get-e2e-nginx
IMAGE="nginx:1.27-alpine@sha256:65645c7bb6a0661892a8b03b89d0743208a18dd2f3f17a54ef4b76fb8e2f2a10"
ARTIFACTS="$PWD/artifacts"
mkdir -p "$ARTIFACTS" certs

# Fixture TLS certificate for proxy.example.com (regenerate with: rm -r certs).
if [ ! -s certs/proxy.example.com.crt ]; then
  openssl req -x509 -newkey rsa:2048 -nodes -days 30 \
    -keyout certs/proxy.example.com.key -out certs/proxy.example.com.crt \
    -subj "/CN=proxy.example.com" \
    -addext "subjectAltName=DNS:proxy.example.com" >/dev/null 2>&1
fi

# A previous fixture run still owns the port; replace it.
pkill -f 'get_carrier_browser_fixture' 2>/dev/null || true
sleep 0.5

# Real WEB HTTP stack on 0.0.0.0:$PORT with the test echo backend enabled.
# setsid detaches it from this shell so it survives the runner's exit.
setsid nohup env \
  TELEMT_WEB_E2E_FIXTURE=1 \
  TELEMT_WEB_GET_E2E_ECHO=1 \
  TELEMT_WEB_E2E_CARRIER="$CARRIER" \
  TELEMT_WEB_E2E_PORT="$PORT" \
  CARGO_TARGET_DIR=/home/rerowros/telemt-target \
  cargo test --manifest-path ../../Cargo.toml --bin telemt \
  get_carrier_browser_fixture -- --ignored --nocapture \
  >"$ARTIFACTS/telemt-$CARRIER.log" 2>&1 < /dev/null &
TELEMT_PID=$!
echo "telemt fixture pid=$TELEMT_PID log=$ARTIFACTS/telemt-$CARRIER.log"

CAP=""
for _ in $(seq 1 480); do
  CAP=$(grep -oP 'capability=\K\S+' "$ARTIFACTS/telemt-$CARRIER.log" 2>/dev/null || true)
  [ -n "$CAP" ] && break
  kill -0 "$TELEMT_PID" 2>/dev/null || { echo "fixture exited early"; cat "$ARTIFACTS/telemt-$CARRIER.log"; exit 1; }
  sleep 0.5
done
[ -n "$CAP" ] || { echo "fixture did not become ready"; exit 1; }
echo "capability=$CAP carrier=$CARRIER"

docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
docker run -d --name "$CONTAINER" \
  -p 443:443 -p "$PARENT_PORT:$PARENT_PORT" \
  -v "$PWD/nginx.conf:/etc/nginx/nginx.conf:ro" \
  -v "$PWD/certs:/etc/nginx/certs:ro" \
  -v "$PWD/www:/e2e-www:ro" \
  "$IMAGE" >/dev/null
echo "nginx container=$CONTAINER https://proxy.example.com:443 http://127.0.0.1:$PARENT_PORT"

for _ in $(seq 1 60); do
  code=$(curl -ks -o /dev/null -w '%{http_code}' \
    --resolve "proxy.example.com:443:127.0.0.1" "https://proxy.example.com/" || true)
  [ "$code" = "200" ] && break
  sleep 0.5
done
[ "${code:-}" = "200" ] || { echo "edge did not become ready (last=$code)"; docker logs "$CONTAINER"; exit 1; }

# Playwright runs on the Windows host where the pinned browsers live.
WIN_DIR=$(wslpath -w "$PWD")
node.exe "$WIN_DIR\\e2e.mjs" "$CAP" "$CARRIER" "$WIN_DIR\\artifacts"
RC=$?
echo "e2e result carrier=$CARRIER rc=$RC artifacts=$ARTIFACTS"
exit "$RC"
