#!/bin/bash
# swap-hub.sh — atomic perry-hub binary swap with health gate and auto-rollback.
#
# Runs ON the hub server (uploaded + invoked by .github/workflows/release.yml,
# also usable by hand). Usage:
#
#   swap-hub.sh /path/to/perry-hub.new [expected-version]
#
# If expected-version is given, the health gate additionally asserts that
# /api/v1/status reports that hub_version — proving the new binary (not a
# cached/old one) is what came up.
#
# Exit 0 only if the NEW binary is live and healthy. Any failure rolls back
# to the previous binary and exits 1 (the rollback outcome is reported but
# never masks the failure).
set -u

HUB_DIR="/opt/perry-hub"
BIN="$HUB_DIR/perry-hub"
SERVICE="perry-hub"
HTTP_PORT="${PERRY_HUB_HTTP_PORT:-3456}"
WS_PORT="${PERRY_HUB_WS_PORT:-3457}"
STATUS_URL="http://127.0.0.1:${HTTP_PORT}/api/v1/status"
HEALTH_TRIES=30          # x1s = 30s for the service to come up healthy
KEEP_BACKUPS=5

NEW_BIN="${1:?usage: swap-hub.sh /path/to/perry-hub.new [expected-version]}"
EXPECTED_VERSION="${2:-}"

fail() { echo "swap-hub: ERROR: $*" >&2; exit 1; }

# --- Preconditions ------------------------------------------------------
[ -f "$NEW_BIN" ] || fail "new binary not found: $NEW_BIN"
file "$NEW_BIN" | grep -q 'ELF 64-bit' || fail "$NEW_BIN is not an ELF binary: $(file -b "$NEW_BIN")"
chmod +x "$NEW_BIN"

# A binary that can't even be exec'd on THIS machine (glibc mismatch etc.)
# must never reach the swap. --help isn't supported; exec'ing with an
# invalid flag still proves the loader accepts it (anything but 126/127).
"$NEW_BIN" --__swap_hub_exec_probe >/dev/null 2>&1 &
PROBE_PID=$!
sleep 1
kill "$PROBE_PID" >/dev/null 2>&1
wait "$PROBE_PID" 2>/dev/null
PROBE_RC=$?
if [ "$PROBE_RC" = "126" ] || [ "$PROBE_RC" = "127" ]; then
  fail "new binary failed to exec on this host (rc=$PROBE_RC — loader/glibc problem?)"
fi

health_ok() {
  # Service alive + HTTP status ok + WS port accepting connections.
  systemctl is-active --quiet "$SERVICE" || return 1
  local body
  body=$(curl -sf -m 5 "$STATUS_URL" 2>/dev/null) || return 1
  echo "$body" | grep -q '"status":"ok"' || return 1
  if [ -n "$EXPECTED_VERSION" ] && [ "${CHECK_VERSION:-1}" = "1" ]; then
    echo "$body" | grep -q "\"hub_version\":\"$EXPECTED_VERSION\"" || return 1
  fi
  (exec 3<>"/dev/tcp/127.0.0.1/$WS_PORT") 2>/dev/null || return 1
  exec 3>&- 2>/dev/null
  return 0
}

wait_healthy() {
  local i
  for i in $(seq 1 "$HEALTH_TRIES"); do
    sleep 1
    health_ok && return 0
  done
  return 1
}

# --- Backup + swap ------------------------------------------------------
TS=$(date +%s)
BACKUP="$BIN.bak.$TS"
echo "swap-hub: backing up current binary to $BACKUP"
cp -p "$BIN" "$BACKUP" || fail "could not back up current binary"

# Prune old backups, keep the newest $KEEP_BACKUPS.
ls -1t "$BIN".bak.* 2>/dev/null | tail -n "+$((KEEP_BACKUPS + 1))" | xargs -r rm -f

echo "swap-hub: swapping in new binary"
mv -f "$NEW_BIN" "$BIN" || fail "could not move new binary into place"
systemctl restart "$SERVICE"

if wait_healthy; then
  echo "swap-hub: OK — new hub is live and healthy"
  curl -sf -m 5 "$STATUS_URL" || true
  echo
  exit 0
fi

# --- Rollback -----------------------------------------------------------
echo "swap-hub: HEALTH GATE FAILED — rolling back to $BACKUP" >&2
echo "swap-hub: recent logs from the failed binary:" >&2
journalctl -u "$SERVICE" --since '2 minutes ago' --no-pager -n 40 >&2 || true

cp -p "$BACKUP" "$BIN"
systemctl restart "$SERVICE"
# Old binary won't report the new version — skip the version assertion.
if CHECK_VERSION=0 wait_healthy; then
  echo "swap-hub: rollback succeeded — previous binary is live again" >&2
else
  echo "swap-hub: ROLLBACK ALSO UNHEALTHY — manual intervention required!" >&2
  echo "swap-hub: fallback: run deploy.sh from the repo (builds on the worker, bypasses the hub pipeline entirely)" >&2
  journalctl -u "$SERVICE" --since '1 minute ago' --no-pager -n 40 >&2 || true
fi
exit 1
