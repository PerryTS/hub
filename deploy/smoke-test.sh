#!/bin/bash
# smoke-test.sh — boot a freshly built perry-hub binary and prove it serves
# before it is allowed anywhere near the production server.
#
#   smoke-test.sh /path/to/perry-hub [expected-version]
#
# Probes:
#   1. process survives startup (no immediate crash)
#   2. GET /api/v1/status → 200, "status":"ok", sane JSON fields
#   3. hub_version matches the release tag (when given)
#   4. WS port completes a real RFC6455 upgrade handshake (101)
#   5. admin endpoint rejects unauthenticated requests (403)
#   6. process is still alive after all probes (no crash-on-first-request)
#
# Environment is self-contained: high ports, throwaway secrets. A MySQL on
# 127.0.0.1:3306 is optional — the pool connects lazily and none of the
# probed endpoints touch it.
set -u

BIN="${1:?usage: smoke-test.sh /path/to/perry-hub [expected-version]}"
EXPECTED_VERSION="${2:-}"

export PERRY_HUB_HTTP_PORT="${PERRY_HUB_HTTP_PORT:-13456}"
export PERRY_HUB_WS_PORT="${PERRY_HUB_WS_PORT:-13457}"
export PERRY_HUB_WORKER_SECRET="smoke-test-worker-secret"
export PERRY_HUB_ADMIN_SECRET="smoke-test-admin-secret"
export PERRY_DB_HOST="${PERRY_DB_HOST:-127.0.0.1}"
export PERRY_DB_USER="${PERRY_DB_USER:-perry}"
export PERRY_DB_PASSWORD="${PERRY_DB_PASSWORD:-smoke}"
export PERRY_DB_NAME="${PERRY_DB_NAME:-perry_hub}"

STATUS_URL="http://127.0.0.1:${PERRY_HUB_HTTP_PORT}/api/v1/status"
LOG=$(mktemp -t hub-smoke.XXXX.log)
HUB_PID=""

cleanup() {
  [ -n "$HUB_PID" ] && kill "$HUB_PID" 2>/dev/null
  wait 2>/dev/null
}
trap cleanup EXIT

fail() {
  echo "smoke-test: FAIL: $*" >&2
  echo "--- hub output ---------------------------------------------" >&2
  cat "$LOG" >&2
  exit 1
}

chmod +x "$BIN"
echo "smoke-test: starting $BIN (http :$PERRY_HUB_HTTP_PORT, ws :$PERRY_HUB_WS_PORT)"
"$BIN" >"$LOG" 2>&1 &
HUB_PID=$!

# 1. survives startup + comes up within 30s
BODY=""
for i in $(seq 1 30); do
  sleep 1
  kill -0 "$HUB_PID" 2>/dev/null || fail "hub process died during startup"
  BODY=$(curl -sf -m 5 "$STATUS_URL" 2>/dev/null) && break
done
[ -n "$BODY" ] || fail "status endpoint never came up"
echo "smoke-test: status: $BODY"

# 2. status content
echo "$BODY" | grep -q '"status":"ok"' || fail 'status missing "status":"ok"'
echo "$BODY" | grep -q '"supported_targets":\[' || fail 'status missing supported_targets array'
echo "$BODY" | grep -q '"connected_workers":' || fail 'status missing connected_workers'

# 3. version stamp
if [ -n "$EXPECTED_VERSION" ]; then
  echo "$BODY" | grep -q "\"hub_version\":\"$EXPECTED_VERSION\"" \
    || fail "hub_version is not $EXPECTED_VERSION (version sed did not take?)"
fi

# 4. real WS upgrade handshake on the worker port
node - "$PERRY_HUB_WS_PORT" <<'EOF' || fail "WS upgrade handshake failed"
const http = require('http');
const port = process.argv[2];
const req = http.request({
  host: '127.0.0.1', port,
  headers: {
    'Connection': 'Upgrade', 'Upgrade': 'websocket',
    'Sec-WebSocket-Version': '13',
    'Sec-WebSocket-Key': 'dGhlIHNhbXBsZSBub25jZQ==',
  },
});
req.on('upgrade', (res, socket) => {
  console.log('smoke-test: WS upgrade OK (101)');
  socket.destroy();
  process.exit(0);
});
req.on('response', (res) => { console.error('expected 101, got HTTP ' + res.statusCode); process.exit(1); });
req.on('error', (e) => { console.error(e.message); process.exit(1); });
req.end();
setTimeout(() => { console.error('WS handshake timeout'); process.exit(1); }, 5000);
EOF

# 5. admin auth is enforced
CODE=$(curl -s -o /dev/null -w '%{http_code}' -m 5 -X POST \
  "http://127.0.0.1:${PERRY_HUB_HTTP_PORT}/api/v1/admin/update-perry")
[ "$CODE" = "403" ] || fail "unauthenticated admin endpoint returned $CODE, expected 403"

# 5b. headless job endpoints reject cleanly (401), not crash (500). Catches
# the auto-optimize compile-mode regression (perry 0.5.1159, worker docker
# builds) where the variant fastify lost the request object's .headers
# across a function boundary and these routes 500'd.
for ep in "jobs/00000000-0000-0000-0000-000000000000/status" "jobs/00000000-0000-0000-0000-000000000000/artifact"; do
  CODE=$(curl -s -o /dev/null -w '%{http_code}' -m 5 \
    "http://127.0.0.1:${PERRY_HUB_HTTP_PORT}/api/v1/$ep")
  [ "$CODE" = "401" ] || fail "unauthenticated /$ep returned $CODE, expected 401"
done

# 6. still alive after the probes
sleep 2
kill -0 "$HUB_PID" 2>/dev/null || fail "hub crashed after serving requests"

echo "smoke-test: PASS — binary boots, serves HTTP + WS, enforces auth, stays up"
