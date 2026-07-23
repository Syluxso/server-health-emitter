# server-health-emitter

Tiny Go service for Byzantine Admin:

- Consumes Kafka topic `byz.gateway.access` (gateway access events)
- Streams them over **SSE** (+ JSON “recent” ring buffer)
- Also streams **host CPU/RAM** metrics on the same SSE connection

Lightweight ops bridge — not a full observability platform.

**Admin integration contract:** [docs/ADMIN-INTEGRATION.md](docs/ADMIN-INTEGRATION.md)

## Endpoints

| Path | Auth | Description |
|------|------|-------------|
| `GET /api/v1/gateway-access/stream` | Bearer JWT | SSE: gateway events + host metrics + traffic |
| `GET /api/v1/gateway-access/recent` | Bearer JWT | Last ~50 gateway events (JSON) |
| `GET /api/v1/gateway-access/stats` | Bearer JWT | Ingest RPS + 20s buckets (not limited by ring) |
| `GET /api/v1/host-metrics` | Bearer JWT | One-shot host sample (JSON) |
| `GET /healthz` | none | Liveness |

### SSE event names

| `event:` | Meaning |
|----------|---------|
| `snapshot` | Gateway request history on connect |
| `gateway.request.completed` | Live gateway request |
| `gateway.traffic` | Ingest rate (~every 1s): `rps`, `peakRps`, `buckets[20]` |
| `host.metrics` | Host CPU/RAM/load (~every 2s) |

RPS/traffic is counted **as Kafka messages are consumed**, so a 1000/s burst shows ~1000 on the meter even though the live list only keeps ~50 rows.

### Example `host.metrics`

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

## Auth

```
Authorization: Bearer <IAM RS256 access token>
```

JWT is validated against JWKS (`IAM_JWKS_URL`).

**Angular / browser:** native `EventSource` cannot send `Authorization`. Use:
- `fetch()` + stream reader for SSE, or
- poll `/api/v1/gateway-access/recent` every 1–2s

## Config (env)

| Env | Default |
|-----|---------|
| `PORT` | `8097` |
| `KAFKA_BOOTSTRAP` | `127.0.0.1:9092` |
| `KAFKA_TOPIC` | `byz.gateway.access` |
| `KAFKA_GROUP` | `admin-gateway-sse` |
| `IAM_JWKS_URL` | `http://127.0.0.1:8082/.well-known/jwks.json` |
| `CORS_ORIGINS` | `http://localhost:4200,https://admin.byzantineapp.dev` |
| `HOST_METRICS_INTERVAL` | `2s` |

Listens on **`127.0.0.1` only** — put nginx (TLS) in front for public Admin.

## Build & run

```bash
go mod tidy
make build
# or: CGO_ENABLED=0 go build -o admin-gateway-sse .

export KAFKA_BOOTSTRAP=127.0.0.1:9092
export IAM_JWKS_URL=http://127.0.0.1:8082/.well-known/jwks.json
./admin-gateway-sse
```

## Production layout (Byzantine Linode)

```
/opt/services/admin-gateway-sse/admin-gateway-sse   # binary
Supervisor program: admin-gateway-sse
Public URL: https://api.byzantineapp.dev/ops/...
```

nginx: `location /ops/` → `http://127.0.0.1:8097/`

Kafka consumer group: **`admin-gateway-sse`** (do not reuse for other services).

### Jenkins

Root [`Jenkinsfile`](Jenkinsfile): `go build` → copy binary (+ `start.sh`) to `/opt/services/admin-gateway-sse/` → `supervisorctl restart admin-gateway-sse`.

Agent needs **Go** on `PATH` and sudoers for mkdir/cp/chown/supervisorctl (same pattern as other Byz services). Point a Pipeline job at this repo / `Jenkinsfile`.

## Public Admin URLs (Byzantine)

```
https://api.byzantineapp.dev/ops/api/v1/gateway-access/stream
https://api.byzantineapp.dev/ops/api/v1/gateway-access/recent
https://api.byzantineapp.dev/ops/api/v1/host-metrics
```

## License

Public — Syluxso / Byzantine.
