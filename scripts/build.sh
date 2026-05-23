#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PERRY_BIN="${PERRY:-perry}"
OUT="${1:-$ROOT/perry-hub}"

"$ROOT/scripts/check-perry-runtime.sh"

exec "$PERRY_BIN" compile "$ROOT/src/main.ts" -o "$OUT" --no-cache
