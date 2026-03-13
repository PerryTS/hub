# Perry Hub

Public-facing build server for the [Perry](https://github.com/PerryTS/perry) ecosystem. Receives build uploads from the `perry publish` CLI, manages licenses, and dispatches build jobs to workers via WebSocket.

## How It Works

```
perry publish ──► Perry Hub ──► Worker (macOS/iOS/Android)
  (CLI)         (this server)       │
                     ▲              │
                     └──────────────┘
                     artifacts + status
```

1. A developer runs `perry publish` which uploads a tarball to Perry Hub
2. Perry Hub validates the license, rate-limits, and enqueues a build job
3. A connected worker picks up the job, downloads the tarball, and builds it
4. The worker uploads artifacts back to Perry Hub
5. The CLI downloads the finished artifacts

## Tech Stack

- **TypeScript** compiled to a native binary by the [Perry compiler](https://github.com/PerryTS/perry) (not Node.js)
- **Fastify** for HTTP
- **ws** for WebSocket
- **MySQL** for license and build persistence

## Project Structure

```
src/main.ts      # Entire server (single file)
perry.toml       # Perry compiler project config
perry-hub        # Compiled binary (gitignored)
```

## Building

Requires the [Perry compiler](https://github.com/PerryTS/perry):

```sh
perry compile src/main.ts -o perry-hub
```

## Running

```sh
# Set required environment variables (see Configuration below)
export PERRY_DB_PASSWORD=...
export PERRY_HUB_WORKER_SECRET=...

./perry-hub
```

## Configuration

All configuration is via environment variables:

| Variable | Default | Description |
|---|---|---|
| `PERRY_HUB_HTTP_PORT` | `3456` | HTTP server port |
| `PERRY_HUB_WS_PORT` | `3457` | WebSocket server port |
| `PERRY_HUB_WORKER_SECRET` | *(empty)* | Shared secret for worker authentication |
| `PERRY_HUB_ADMIN_SECRET` | *(empty)* | Secret for admin endpoints |
| `PERRY_HUB_PUBLIC_URL` | `https://hub.perryts.com` | Public URL for artifact download links |
| `PERRY_HUB_SELF_HOSTED` | `false` | Set to `true` to disable rate limiting |
| `PERRY_HUB_ARTIFACT_TTL_SECS` | `600` | Seconds before artifacts expire |
| `PERRY_DB_HOST` | `localhost` | MySQL host |
| `PERRY_DB_PORT` | `3306` | MySQL port |
| `PERRY_DB_USER` | `perry` | MySQL user |
| `PERRY_DB_PASSWORD` | *(empty)* | MySQL password |
| `PERRY_DB_NAME` | `perry_hub` | MySQL database name |

## API

### HTTP Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/v1/status` | Server status, queue length, connected workers |
| `POST` | `/api/v1/build` | Submit a build (multipart: license_key, manifest, tarball_b64) |
| `POST` | `/api/v1/license/register` | Register a new license (admin) |
| `POST` | `/api/v1/license/verify` | Verify a license key |
| `GET` | `/api/v1/dl/:token` | Download a build artifact |

### WebSocket Protocol

Workers connect and send `worker_hello` with capabilities. CLI clients connect and send `subscribe` with a `job_id` to receive real-time build updates.

## Related Repos

- [perry](https://github.com/PerryTS/perry) — The Perry compiler and CLI
- [builder-macos](https://github.com/PerryTS/builder-macos) — macOS/iOS/Android build worker

## License

MIT
