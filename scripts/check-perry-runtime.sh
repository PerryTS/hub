#!/usr/bin/env bash
set -euo pipefail

MIN_PERRY_VERSION="0.5.1026"
PERRY_BIN="${PERRY:-perry}"

version_ge() {
  local have_major have_minor have_patch
  local want_major want_minor want_patch

  IFS=. read -r have_major have_minor have_patch <<< "$1"
  IFS=. read -r want_major want_minor want_patch <<< "$2"

  [[ "$have_major" =~ ^[0-9]+$ ]] || return 1
  [[ "$have_minor" =~ ^[0-9]+$ ]] || return 1
  [[ "$have_patch" =~ ^[0-9]+$ ]] || return 1
  [[ "$want_major" =~ ^[0-9]+$ ]] || return 1
  [[ "$want_minor" =~ ^[0-9]+$ ]] || return 1
  [[ "$want_patch" =~ ^[0-9]+$ ]] || return 1

  (( have_major > want_major )) && return 0
  (( have_major < want_major )) && return 1
  (( have_minor > want_minor )) && return 0
  (( have_minor < want_minor )) && return 1
  (( have_patch >= want_patch ))
}

if ! version_output="$("$PERRY_BIN" --version 2>/dev/null)"; then
  echo "error: unable to execute Perry compiler: $PERRY_BIN" >&2
  echo "Set PERRY=/path/to/perry or put Perry on PATH." >&2
  exit 1
fi

version="${version_output#perry }"
version="${version%% *}"
version_core="${version%%-*}"
version_core="${version_core%%+*}"

if ! version_ge "$version_core" "$MIN_PERRY_VERSION"; then
  cat >&2 <<EOF
error: Perry Hub must be built with Perry >= $MIN_PERRY_VERSION.

Found: $version_output

Perry 0.5.1025 predates the GC unsafe-zone runtime fix needed by this
long-running Fastify/WebSocket server. That runtime can suppress the hub's
periodic manual gc() safety valve while still allowing automatic GC in native
server callbacks, which reintroduces the leak/crash reported in Perry issue
#1467. Rebuild Perry from main after PerryTS/perry#1429, then rebuild hub.
EOF
  exit 1
fi

echo "Using $version_output"
