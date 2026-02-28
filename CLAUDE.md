# Perry Hub

Public-facing build server for the Perry ecosystem. Receives build uploads from
the `perry publish` CLI, manages licenses, and dispatches build jobs to workers
via WebSocket.

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
- **Licenses**: in-memory Map, persisted to `/tmp/perry-hub-data/licenses.json`
- **Job queue**: priority-ordered (pro tier > free tier)
- **Worker pool**: workers connect via WS, identified by platform capabilities
- **Artifacts**: temp files in `/tmp/perry-artifacts/`, auto-cleaned after TTL
- **Rate limiting**: per-license concurrent + hourly limits

## Related Repos
- [perry](https://github.com/PerryTS/perry) — compiler + CLI
- [builder-macos](https://github.com/PerryTS/builder-macos) — macOS build worker
