# CS2 Demo Viewer Server

A lightweight Go HTTP server that serves the web application and proxies demo file downloads.

## Overview

This server component provides two main functions:
1. **Static file serving**: Serves the built web application (HTML, JS, CSS, WASM files)
2. **Demo download proxy**: Securely proxies demo file downloads from external sources (e.g., FACEIT)
3. **Webhook ingest pipeline**: Accepts Faceit demo-ready events, downloads demos in background workers, and exposes replay readiness/status endpoints

## Technology Stack

- **Language**: Go 1.25.1
- **HTTP Server**: Standard library `net/http`

## Development

### Prerequisites

- Go 1.25.1 or higher

### Running the Server

#### Development mode (from project root)

```bash
make server
```

Or directly:

```bash
go run server/main.go -dev
```

Development mode enables:
- CORS headers for cross-origin requests
- Relaxed URL validation for demo downloads
- Useful for local development with the web frontend

#### Production mode

```bash
go run server/main.go
```

Production mode:
- Restricts demo URLs to allowed domains for security
- No CORS headers
- Suitable for deployment

### Testing

Run tests from the server directory:

```bash
cd server
go test -v ./...
```

### Building

Build a production binary:

```bash
go build -o csgo-server server/main.go
```

## API Endpoints

### `GET /download?url=<demo_url>`

Proxies demo file downloads from external sources.

**Query Parameters:**
- `url`: The URL of the demo file to download

**Example:**
```bash
curl "http://localhost:8080/download?url=https://example.com/demo.dem.zst"
```

### `POST /webhooks/faceit/demo-ready`

Receives a Faceit `match_demo_ready` webhook payload and enqueues demo ingestion.

**Headers:**
- `<WEBHOOK_HEADER_NAME>`: Must match configured secret value

**Required environment variables:**
- `WEBHOOK_HEADER_NAME` (optional, default `X-Webhook-Secret`)
- `WEBHOOK_HEADER_VALUE` (required)

**Behavior:**
- Validates event and payload
- Deduplicates by webhook `event_id`
- Returns `202 Accepted` when queued

### `GET /replays/<match_id>/status`

Returns replay state for a match.

**States:**
- `missing`
- `queued`
- `parsing`
- `ready`
- `failed`

### `GET /replays/<match_id>`

Read-only replay fetch endpoint. Returns prepared replay artifact when state is `ready`. Returns `202` with state payload while processing.

### `POST /admin/replays/reprocess`

Admin-only manual reprocess endpoint.

**Headers:**
- `X-Admin-Token`: Must match `ADMIN_REPROCESS_TOKEN`

**Body:**
- `match_id` (required)
- `demo_url` (required)
- `map_id` (optional)

### `POST /admin/replays/reprocess-failed`

Admin-only bulk reprocess endpoint for records currently in `failed` state.

**Headers:**
- `X-Admin-Token`: Must match `ADMIN_REPROCESS_TOKEN`

**Body (all optional):**
- `match_id`: Limit to one match
- `map_id`: Limit to one map id
- `limit`: Maximum number of failed records to process (`0` = no limit)
- `dry_run`: If `true`, returns counts without queueing

**Behavior:**
- Only records in `failed` state are considered.
- Records without `demo_url` are skipped.
- Returns queueing summary counters (`failed_scanned`, `failed_matched`, `failed_selected`, `queued`, `queue_full`, `skipped_no_demo_url`).

### `POST /admin/replays/delete`

Admin-only endpoint to remove a replay record from persisted state.

**Headers:**
- `X-Admin-Token`: Must match `ADMIN_REPROCESS_TOKEN`

**Body:**
- `match_id` (required)
- `map_id` (required)

**Behavior:**
- Removes the replay key `<match_id>::<map_id>` from `records`.
- Removes matching dedupe keys from `seen_events`.
- Removes the replay key from `queue_order`.
- Recomputes `latest_map_by_match` for the match.

### Static File Serving

All other requests serve static files from the web application build directory.

## Configuration

- `-dev`: Enable development mode (default: `false`)
- `WEBHOOK_HEADER_NAME`: Static webhook auth header name (default: `X-Webhook-Secret`)
- `WEBHOOK_HEADER_VALUE`: Static webhook auth header value (required for webhook ingest)
- `ADMIN_REPROCESS_TOKEN`: Static admin token for manual reprocess endpoint
- `REPLAYS_DIR`: Filesystem directory for replay artifacts and metadata (default: `./parsed`)
- `ALLOWED_ORIGINS`: Comma-separated CORS allowlist for production `GET /download`

## How It Works

1. **Static Files**: The server serves pre-built web application files from the `dist` directory
2. **Demo Downloads**: When a user requests a demo file:
   - The browser sends a request to `/download?url=<demo_url>`
   - The server validates the URL (in production mode)
   - The server fetches the file from the external source
   - The file is streamed back to the client in chunks
3. **CORS**: In dev mode, CORS headers allow the web app to make requests from different origins
4. **Webhook Ingest**:
   - Faceit webhook hits `/webhooks/faceit/demo-ready`
   - Server validates static secret header and payload
   - Background worker downloads demo and stores artifact under `parsed/<match_id>/`
   - Client can poll `/replays/<match_id>/status` and load `/replays/<match_id>` when ready
