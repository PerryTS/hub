# Perry Hub

Public-facing build server for the Perry ecosystem. Receives build uploads from
the `perry publish` CLI, manages licenses, and dispatches build jobs to workers
via WebSocket. Orchestrates the multi-stage build pipeline: Linux worker compiles
all platforms, then hub re-queues precompiled bundles as sign-only jobs to
platform-specific sign workers (macOS worker for ios-sign/macos-sign, Azure VM
for windows-sign).

## Tech Stack
- **TypeScript** compiled to native binary by the [perry compiler](https://github.com/PerryTS/perry)
- **Not Node.js** — perry compiles TS to native code via Cranelift. No npm, no node_modules.
- HTTP: Fastify (port 3456)
- WebSocket: ws module (port 3457)

## Project Structure
```
src/main.ts      # Entire server (single file)
perry.toml       # Perry compiler project config
perry-hub        # Compiled binary (gitignored)
```

## Build & Run
```sh
# Compile (requires perry compiler)
~/projects/perry/target/release/perry compile src/main.ts -o perry-hub

# Run
./perry-hub
```

## Perry Compiler Quirks
This code is compiled by perry, NOT by tsc/node. The perry compiler has known
limitations that heavily influence code style:

- **Use Maps, not arrays** for stateful collections (array push corrupts values)
- **Track `.length` manually** (`.length` on module-level arrays returns garbage)
- **Module-level `let` doesn't work in closures** — use `const obj = { x: 0 }` instead
- **Return `JSON.stringify()` strings** from Fastify handlers (not objects)
- **`app.listen()` never returns** — register all handlers before calling it
- **No binary data in HTTP bodies** — pass file paths instead
- **`crypto.randomUUID()`** instead of `crypto.randomBytes().toString('hex')`
- **WebSocket**: use server-level events (`wss.on`), not per-connection (`ws.on`)

See memory file `perry-quirks.md` for the full list.

## Key Subsystems
- **Licenses**: MySQL-backed, cached in-memory Map
- **Job queue**: priority-ordered (pro tier > free tier)
- **Worker pool**: workers connect via WS, identified by platform capabilities
- **Slot-based dispatch**: `workerActiveJobsMap` / `workerMaxConcurrentMap` for concurrent builds
- **Re-queue pipeline**: Linux worker uploads precompiled bundles → hub re-queues as `ios-sign`/`macos-sign`/`windows-sign` to appropriate sign workers
- **Artifacts**: temp files in `/tmp/perry-artifacts/`, auto-cleaned after TTL
- **Rate limiting**: per-license concurrent + hourly limits

## Environment Variables
- `PERRY_HUB_HTTP_PORT` — HTTP port (default: 3456)
- `PERRY_HUB_WS_PORT` — WebSocket port (default: 3457)
- `PERRY_HUB_WORKER_SECRET` — shared secret for worker authentication
- `PERRY_HUB_ADMIN_SECRET` — secret for admin endpoints (license registration)
- `PERRY_HUB_PUBLIC_URL` — public-facing URL for download links
- `PERRY_HUB_SELF_HOSTED` — set to `true` to disable rate limiting
- `PERRY_HUB_ARTIFACT_TTL_SECS` — artifact expiry (default: 600)
- `PERRY_DB_HOST`, `PERRY_DB_PORT`, `PERRY_DB_USER`, `PERRY_DB_PASSWORD`, `PERRY_DB_NAME` — MySQL connection

## Workers
- **Linux worker** (84.32.98.120): compiles ALL platforms (linux, android, windows, ios, macos)
- **macOS worker** (oakhost-tart): sign-only for `macos-sign` and `ios-sign` jobs
- **Windows Azure VM** (perry-sign-win): sign-only for `windows-sign` jobs, auto-start/stop

## Related Repos
- [perry](https://github.com/PerryTS/perry) — compiler + CLI
- [builder-linux](https://github.com/PerryTS/builder-linux) — Linux worker (all compilation)
- [builder-macos](https://github.com/PerryTS/builder-macos) — macOS/iOS sign-only worker
