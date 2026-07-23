# Admin System Health — ops SSE / API integration

How the **Admin** app (sys / admin UI) talks to **server-health-emitter** (`admin-gateway-sse` on the Linode).

This is **ops visualization only**. It is not the long-term logging product. Gateway keeps producing Kafka events either way; if this process is down, APIs still work — Admin just has no live feed.

---

## Base URL

All public calls go through the API gateway host, under `/ops/`:

```text
https://api.byzantineapp.dev/ops
```

nginx proxies `/ops/` → `http://127.0.0.1:8097/` (Go service).

Local (on the server only):

```text
http://127.0.0.1:8097
```

---

## Auth (required)

Every data endpoint (not `/healthz`) needs:

```http
Authorization: Bearer <IAM access token>
```

- Token must be a valid **IAM RS256 JWT** (same login as the rest of the platform).
- Server validates via JWKS at IAM (`http://127.0.0.1:8082/.well-known/jwks.json` on the box).
- Missing / invalid token → **401**.

### Browser caveat (important)

Native **`EventSource` cannot set `Authorization`**.

Admin must either:

1. Prefer **`fetch()` + ReadableStream** for SSE and pass the Bearer header, or
2. **Poll** the JSON endpoints every 1–2s with normal `HttpClient` + Bearer.

Do **not** put the token in the query string (leaks into logs).

---

## Endpoints Admin should use

| Purpose | Method | Public URL |
|---------|--------|------------|
| Live stream (requests + CPU/RAM + RPS) | `GET` | `https://api.byzantineapp.dev/ops/api/v1/gateway-access/stream` |
| Last ~50 gateway requests | `GET` | `https://api.byzantineapp.dev/ops/api/v1/gateway-access/recent` |
| Traffic / RPS snapshot | `GET` | `https://api.byzantineapp.dev/ops/api/v1/gateway-access/stats` |
| One-shot host CPU/RAM | `GET` | `https://api.byzantineapp.dev/ops/api/v1/host-metrics` |
| Process liveness (no auth) | `GET` | `https://api.byzantineapp.dev/ops/healthz` |

### Recommended UI wiring

| Dashboard widget | Source |
|------------------|--------|
| Live request rows | SSE `snapshot` + `gateway.request.completed`, **or** poll `.../recent` |
| RPS / traffic chart | SSE `gateway.traffic`, **or** poll `.../stats` every 1s |
| CPU / RAM gauges | SSE `host.metrics`, **or** poll `.../host-metrics` every 2s |

Using **SSE alone** for all three is fine if fetch-streaming is implemented. Polling is simpler and works well for low traffic.

---

## SSE: event names and payloads

Connect:

```http
GET /ops/api/v1/gateway-access/stream
Authorization: Bearer <token>
Accept: text/event-stream
```

On connect the server typically sends:

1. Zero or more **`snapshot`** frames (recent gateway requests, up to ~50)
2. An initial **`host.metrics`** (if implemented on connect)
3. Then live frames as things happen / tickers fire

Keepalive: comment lines `: ping` every ~15s — ignore them.

### `event: snapshot` / `event: gateway.request.completed`

JSON body (approx):

```json
{
  "eventId": "uuid",
  "type": "gateway.request.completed",
  "occurredAt": "2026-07-20T23:10:50.073Z",
  "requestId": "uuid",
  "method": "GET",
  "path": "/iam/actuator/health",
  "status": 200,
  "durationMs": 36,
  "clientIp": "127.0.0.1",
  "routeId": "iam"
}
```

Use for the “Live Requests” table.
**Ring size ≈ 50** — Admin will not get full history of a 1000-event flood, only the latest ~50 rows. Kafka still holds all events for other consumers.

### `event: host.metrics`

```json
{
  "type": "host.metrics",
  "time": "2026-07-20T23:15:00Z",
  "cpuPercent": 12.3,
  "memTotalMb": 3891,
  "memUsedMb": 2100,
  "memAvailableMb": 1200,
  "memUsedPercent": 54.0,
  "swapTotalMb": 2543,
  "swapUsedMb": 400,
  "load1": 0.25,
  "load5": 0.30,
  "load15": 0.28
}
```

Emitted about every **2 seconds** (server config).

### `event: gateway.traffic`

Ingest rate as seen by this consumer (counts **all** Kafka messages, not just the 50 in the ring):

```json
{
  "type": "gateway.traffic",
  "time": "2026-07-22T23:45:49Z",
  "rps": 0,
  "peakRps": 100,
  "total": 103,
  "buckets": [0, 0, /* … length 20, oldest → newest */ 100, 0, 0]
}
```

| Field | Meaning |
|-------|---------|
| `rps` | Rate for the last fully completed 1s bucket (or current if burst still open) |
| `peakRps` | Max 1s bucket in the last ~20s window |
| `total` | Messages consumed since **this process** started |
| `buckets` | 20 integers, one per second, **oldest → newest** |

Emitted about every **1 second**. Use for RPS / sparkline; do **not** derive RPS only from the length of the recent list.

---

## JSON poll alternatives (if not using SSE)

### Recent requests

```http
GET https://api.byzantineapp.dev/ops/api/v1/gateway-access/recent
Authorization: Bearer <token>
```

Response:

```json
{
  "count": 50,
  "events": [ /* same objects as gateway.request.completed */ ]
}
```

### Traffic / RPS

```http
GET https://api.byzantineapp.dev/ops/api/v1/gateway-access/stats
Authorization: Bearer <token>
```

Same body as `gateway.traffic` SSE payload.

### Host metrics

```http
GET https://api.byzantineapp.dev/ops/api/v1/host-metrics
Authorization: Bearer <token>
```

Same body as `host.metrics` SSE payload.

---

## CORS

Allowed origins (server config) currently include:

- `http://localhost:4200`
- `https://admin.byzantineapp.dev`
- `https://sys.byzantineapp.dev`

Preflight must send real `Origin`. If you add another Admin host, it must be added on the server (`CORS_ORIGINS` / start script) and the service restarted.

OPTIONS should return **204** with:

```http
Access-Control-Allow-Origin: https://sys.byzantineapp.dev
Access-Control-Allow-Methods: GET, OPTIONS
Access-Control-Allow-Headers: Authorization, Content-Type
```

---

## Dependencies (server-side — Admin should handle gracefully)

| Dependency | If down |
|------------|---------|
| **Kafka** (`byz kafka up`) | No new gateway access events / traffic; host metrics may still work |
| **IAM** | No JWT validation → all ops calls 401 |
| **admin-gateway-sse** process | `/ops/*` fails; gateway and other APIs still work |
| **api.byzantineapp.dev** nginx | Public path unavailable |

Admin UI should show “stream offline” / empty charts rather than hanging forever (timeouts on fetch/poll).

---

## Minimal Angular checklist

1. After IAM login, keep `accessToken`.
2. System Health page:
   - Open SSE stream with **fetch + Authorization**, **or** poll the three JSON URLs.
3. Map SSE by `event` name (not only `data` content).
4. Live table: cap display to what the API returns (~50); don’t assume full Kafka history.
5. RPS: use `gateway.traffic` / `stats`, not `events.length`.
6. CPU/RAM: use `host.metrics` / `host-metrics`.
7. CORS origin must be exactly the browser origin (e.g. `https://sys.byzantineapp.dev`).
8. On 401: re-login / refresh token.
9. On network error: show degraded state; don’t block the rest of Admin.

---

## Quick curl checks (for developers)

```bash
# health (no auth)
curl -s https://api.byzantineapp.dev/ops/healthz

# after login → TOKEN=...
curl -s -H "Authorization: Bearer $TOKEN" \
  https://api.byzantineapp.dev/ops/api/v1/gateway-access/recent

curl -s -H "Authorization: Bearer $TOKEN" \
  https://api.byzantineapp.dev/ops/api/v1/gateway-access/stats

curl -s -H "Authorization: Bearer $TOKEN" \
  https://api.byzantineapp.dev/ops/api/v1/host-metrics

# SSE (use fetch in browser; curl for smoke test)
curl -N -H "Authorization: Bearer $TOKEN" \
  https://api.byzantineapp.dev/ops/api/v1/gateway-access/stream
```

Generate traffic:

```bash
curl -s https://api.byzantineapp.dev/iam/actuator/health
```

---

## Server ops (not Admin code)

| Item | Value |
|------|--------|
| Supervisor program | `admin-gateway-sse` |
| Listen | `127.0.0.1:8097` |
| Kafka topic | `byz.gateway.access` |
| Consumer group | `admin-gateway-sse` |
| Source / binary | `/opt/services/admin-gateway-sse/` |
| Repo | `git@github.com:Syluxso/server-health-emitter.git` |

```bash
supervisorctl status admin-gateway-sse
byz kafka up    # required for live request feed
```
