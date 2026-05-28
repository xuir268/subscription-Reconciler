# Subscription Reconciler

A subscription entitlement reconciliation service that aggregates access signals from multiple sources (App Store, Carrier, Marketplace) into a single canonical entitlement state per user. Built in Go with PostgreSQL as the sole runtime dependency.

---

## Table of Contents

- [Running the project](#running-the-project)
- [API reference](#api-reference)
- [High-level design](#high-level-design)
- [Component deep-dives](#component-deep-dives)
- [Design decisions](#design-decisions)
- [Tradeoffs considered](#tradeoffs-considered)
- [Observability](#observability)
- [Disaster recovery](#disaster-recovery)
- [What I would change with another week](#what-i-would-change-with-another-week)

---

## Running the project

**Prerequisites:** Docker, Docker Compose.

```bash
# start everything (Postgres, service, Prometheus, Grafana, backup)
docker compose up -d

# verify
curl http://localhost:8080/healthz
# → {"status":"ok"}
```

| Service    | URL                                      |
|------------|------------------------------------------|
| API        | http://localhost:8080                    |
| Grafana    | http://localhost:3000 (admin / admin)    |
| Prometheus | http://localhost:9090                    |
| Postgres   | localhost:5432                           |

**Running tests** (requires a live Postgres):

```bash
TEST_DATABASE_URL="postgres://reconciler:reconciler@localhost:5432/reconciler?sslmode=disable" \
  go test ./internal/service/... -v
```

---

## API reference

### `GET /users/{id}/entitlement`

Returns the canonical current entitlement state for a user.

```bash
curl http://localhost:8080/users/u_42/entitlement
```

```json
{
  "active": true,
  "source": "STORE",
  "expiresAt": "2026-06-27T10:02:06Z",
  "lastChangedAt": "2026-05-28T10:02:06Z",
  "reason": "INITIAL_PURCHASE"
}
```

When no active entitlement exists:

```json
{
  "active": false,
  "source": "NONE",
  "lastChangedAt": "2026-05-28T10:05:38Z",
  "reason": "no active entitlement"
}
```

Source priority: `STORE > CARRIER > MARKETPLACE > NONE`

---

### `POST /webhooks/store`

Ingests an App Store subscription lifecycle event.

```bash
curl -X POST http://localhost:8080/webhooks/store \
  -H "Content-Type: application/json" \
  -d '{
    "eventId":     "evt_001",
    "userId":      "u_42",
    "type":        "INITIAL_PURCHASE",
    "eventTimeMs": 1748426526000,
    "productId":   "premium_monthly"
  }'
```

| Event type       | active | expiresAt  |
|------------------|--------|------------|
| INITIAL_PURCHASE | true   | +30 days   |
| RENEWAL          | true   | +30 days   |
| UN_CANCELLATION  | true   | +30 days   |
| CANCELLATION     | true   | +24h grace |
| BILLING_ISSUE    | true   | +24h grace |
| EXPIRATION       | false  | —          |

Events are deduplicated by `eventId`. Out-of-order delivery is handled by `eventTimeMs` — a late-arriving older event never overrides a newer state. A cancellation or billing issue with an `expiresAt` within 24 hours automatically schedules a `PREMIUM_EXPIRES_SOON` notification.

---

### `POST /webhooks/marketplace/revoke`

Bulk-revokes marketplace entitlements.

```bash
curl -X POST http://localhost:8080/webhooks/marketplace/revoke \
  -H "Content-Type: application/json" \
  -d '{"userIds": ["u_42", "u_99"]}'
```

---

### `GET /admin/config` / `POST /admin/config`

Live runtime configuration. Changes take effect immediately without a restart.

```bash
# read current config
curl http://localhost:8080/admin/config

# tune rate limit live
curl -X POST http://localhost:8080/admin/config \
  -H "Content-Type: application/json" \
  -d '{"key": "api_rate_limit_per_minute", "value": "60"}'

# disable request cache
curl -X POST http://localhost:8080/admin/config \
  -H "Content-Type: application/json" \
  -d '{"key": "api_gateway_enabled", "value": "false"}'
```

| Key                             | Default | Description                         |
|---------------------------------|---------|-------------------------------------|
| `api_rate_limit_per_minute`     | `120`   | Max requests per IP per minute      |
| `api_request_cache_ttl_seconds` | `600`   | Idempotency window in seconds       |
| `api_gateway_enabled`           | `true`  | Toggle the request cache middleware |

---

## High-level design

```
                     ┌────────────────────────────────────────────────────────┐
                     │                    Inbound Traffic                      │
                     │     Store Webhooks · Marketplace · Entitlement Reads   │
                     └──────────────────────────┬─────────────────────────────┘
                                                │
                     ┌──────────────────────────▼─────────────────────────────┐
                     │                   Middleware Chain                      │
                     │                                                         │
                     │  ┌───────────────────────────────────────────────────┐ │
                     │  │ 1. Rate Limiter                                    │ │
                     │  │    Postgres-backed sliding window                  │ │
                     │  │    Keyed per IP × method × path                   │ │
                     │  │    Default 120 req/min · live-tunable via config   │ │
                     │  └──────────────────────────┬────────────────────────┘ │
                     │                             │                           │
                     │  ┌──────────────────────────▼────────────────────────┐ │
                     │  │ 2. Query Suppressor (Postgres Idempotency Cache)   │ │
                     │  │    Applies to POST / PUT / PATCH / DELETE          │ │
                     │  │    Key: Idempotency-Key header or SHA-256(body)    │ │
                     │  │    States: PROCESSING → COMPLETED                  │ │
                     │  │    Replays full response for duplicates in TTL     │ │
                     │  │    Concurrent duplicate → 202 duplicate_in_prog    │ │
                     │  └──────────────────────────┬────────────────────────┘ │
                     │                             │                           │
                     │  ┌──────────────────────────▼────────────────────────┐ │
                     │  │ 3. Instrumentation                                 │ │
                     │  │    Latency histograms · status code counters       │ │
                     │  └──────────────────────────┬────────────────────────┘ │
                     └──────────────────────────────┼─────────────────────────┘
                                                    │
                     ┌──────────────────────────────▼─────────────────────────┐
                     │                    HTTP Handlers                        │
                     │                                                         │
                     │   POST /webhooks/store                                  │
                     │   POST /webhooks/marketplace/revoke                     │
                     │   GET  /users/{id}/entitlement                          │
                     │   GET|POST /admin/config                                │
                     └──────────────────────────────┬─────────────────────────┘
                                                    │
                     ┌──────────────────────────────▼─────────────────────────┐
                     │                 Entitlement Service                     │
                     │                                                         │
                     │   Source priority  STORE > CARRIER > MARKETPLACE        │
                     │   Event ordering   eventTimeMs (stale events rejected)  │
                     │   Webhook dedup    in-memory FIFO 10K entries           │
                     │   All mutations    wrapped in DB transactions            │
                     └──────────┬────────────────────────────┬─────────────────┘
                                │                            │
          ┌─────────────────────▼──────────┐   ┌────────────▼──────────────────┐
          │          PostgreSQL             │   │      Background Workers        │
          │                                │   │                               │
          │  store_events                  │   │  ┌─────────────────────────┐  │
          │  source_entitlements           │   │  │ Carrier Poller          │  │
          │  carrier_poll_jobs             │◀──┤  │ Runs every 5 min        │  │
          │  notifications                 │   │  │ Pool of 8 goroutines    │  │
          │  api_request_cache             │   │  │ FOR UPDATE SKIP LOCKED  │  │
          │  api_rate_limits               │   │  └─────────────────────────┘  │
          │  app_config                    │   │                               │
          └──────────┬─────────────────────┘   │  ┌─────────────────────────┐  │
                     │                         │  │ Notification Sender     │  │
          ┌──────────▼─────────────────────┐   │  │ Runs every 30s          │  │
          │       Backup Service            │   │  │ Marks sent_at           │  │
          │                                 │   │  │ FOR UPDATE SKIP LOCKED  │  │
          │  pg_dump every hour             │   │  └─────────────────────────┘  │
          │  --format=custom --compress=9   │   └───────────────────────────────┘
          │  Retain last 24 snapshots       │
          │  Restore via pg_restore --clean │          ┌──────────────────────┐
          └──────────┬──────────────────────┘          │     Observability    │
                     │                                  │                      │
          ┌──────────▼──────────────────────┐          │  Prometheus  :9090   │
          │       snapshots volume           │          │  Grafana     :3000   │
          └──────────────────────────────────┘          │                      │
                                                        │  Metrics exposed at  │
                                                        │  GET /metrics        │
                                                        └──────────────────────┘
```

---

## Component deep-dives

### Entitlement model

Each user has at most one row per source in `source_entitlements`. The read path queries all active rows ordered by priority and returns the top one as the canonical state. Multiple sources can be active simultaneously — a user with both a STORE subscription and a CARRIER plan sees `source=STORE`.

```
user_id │ source      │ active │ reason         │ expires_at
────────┼─────────────┼────────┼────────────────┼────────────
u_42    │ STORE       │ true   │ RENEWAL        │ 2026-06-27
u_42    │ CARRIER     │ true   │ CARRIER_ACTIVE  │ null
u_42    │ MARKETPLACE │ false  │ MP_REVOKE      │ null

→ GET /users/u_42/entitlement returns source=STORE
```

### Rate limiter

A single `INSERT ... ON CONFLICT DO UPDATE` against `api_rate_limits` atomically increments the counter or resets it when the window has expired. No separate cleanup job is needed — stale rows are overwritten on the next request. The limit is read from `app_config` on every request for live tunability.

### Query suppressor (Postgres idempotency cache)

All mutating requests go through this layer:

1. **First arrival** — insert a row with `state=PROCESSING`; handler runs; response stored as `state=COMPLETED`
2. **Duplicate within TTL** — cached response is replayed directly
3. **Concurrent duplicate** — sees `state=PROCESSING`; returns `202 duplicate_in_progress`
4. **Body hash mismatch on same key** — returns `409 conflict`

Keys are either the `Idempotency-Key` header (caller-supplied) or SHA-256 of the request body (auto-derived). This eliminates the need for a separate Redis cache and keeps idempotency state in the same transaction boundary as the data.

### Carrier poller

`carrier_poll_jobs` is a lightweight distributed work queue. Workers claim rows with `FOR UPDATE SKIP LOCKED`, which is safe across multiple service replicas without advisory locks. Each poll calls `/mock/carrier/plan?userId=...` (the real carrier API in production) and applies the status via `ApplyCarrierStatus`. The mock returns 85% active / 10% inactive / 5% api_error.

### Expiry notification pipeline

```
StoreWebhook arrives
       │
       ▼
storeTransition() computes (active, expiresAt)
       │
       ▼ if expiresAt is within 24h
       │
ScheduleExpiringSoonTx()
  INSERT INTO notifications ... ON CONFLICT DO NOTHING    ← idempotent
       │
       ▼ (every 30s)
NotificationSender.sendOnce()
  UPDATE notifications SET sent_at = now()
  WHERE sent_at IS NULL AND scheduled_for <= now()
  FOR UPDATE SKIP LOCKED
```

The `ON CONFLICT (user_id, entitlement_source, expires_at) DO NOTHING` constraint ensures a user receives each expiry notification at most once, even if the same webhook arrives multiple times.

---

## Design decisions

**Single datastore (PostgreSQL).** Using Postgres for rate limiting, idempotency, job queues, and caching removes the operational burden of running Redis, Kafka, or a separate queue. At this scale (single-region, thousands of users), Postgres handles all of it. The tradeoff is that these concerns share the same connection pool and any Postgres latency spike affects all of them.

**Event ordering by `eventTimeMs`, not insertion order.** Store webhooks frequently arrive out of order due to retries and network conditions. Comparing `eventTimeMs` against the persisted `last_event_time_ms` before applying an update ensures the DB always reflects the most recent logical state, not the most recently received message.

**Source priority computed at read time.** Each source owns its own row and the winner is selected at query time with an `ORDER BY CASE source ...`. This makes partial revocations (marketplace-only) trivially correct and preserves a full per-source audit trail. The alternative — a single merged row updated on every source change — would require fan-out logic and create race conditions between concurrent source updates.

**In-memory FIFO for hot-path webhook dedup.** A 10K-capacity bounded map catches duplicate `eventId`s within a single process before they hit the DB. This is a performance optimisation only — the `store_events.event_id PRIMARY KEY` is the authoritative dedup layer and is always enforced.

**Live config via `app_config` table.** Rate limit and cache TTL are read from the DB on every request, allowing operators to tune behaviour without a deploy. The extra query per request is negligible compared to the main business logic query.

---

## Tradeoffs considered

| Decision | Alternative | Why this choice |
|---|---|---|
| Postgres for rate limiting | Redis | No additional service; Postgres sliding window is sufficient at this scale |
| Postgres for idempotency | Redis + Lua | Transactional consistency with data; no cache invalidation problem |
| `FOR UPDATE SKIP LOCKED` queue | SQS, RabbitMQ | No external dependencies; Postgres already trusted for this data |
| `pg_dump` snapshots | WAL/PITR, pgBackRest | Simpler to operate; portable; RPO of 1h acceptable at current scale |
| Custom Prometheus exporter | `prometheus/client_golang` | Zero external dependencies; full control over label names |
| Source priority at read time | Materialised merged row | Simpler partial revocations; no concurrent update races |
| In-memory FIFO for event dedup | Redis bloom filter | No Redis dependency; covers the hot-traffic window adequately |

---

## Observability

Metrics are exposed at `GET /metrics` in Prometheus text format, scraped by Prometheus every 10 seconds, and visualised in Grafana.

| Metric | Type | Description |
|---|---|---|
| `subscription_api_requests_total` | counter | Requests by method, path, status code |
| `subscription_api_request_duration_seconds` | histogram | Latency with buckets at 5ms–10s |
| `subscription_api_rate_limiter_total` | counter | allowed / blocked / error |
| `subscription_api_request_cache_total` | counter | claimed / replay / in_progress / body_mismatch |
| `subscription_carrier_poll_total` | counter | active / inactive / api_error / apply_error |
| `subscription_notifications_sent_total` | counter | Notifications marked sent by the worker |
| `subscription_carrier_last_batch_size` | gauge | Last carrier poll batch size |
| `go_goroutines` | gauge | Live goroutine count |
| `go_memstats_alloc_bytes` | gauge | Heap allocation |
| `process_cpu_seconds_total` | counter | Total CPU time |

Grafana: http://localhost:3000 (admin / admin)
Prometheus: http://localhost:9090

---

## Disaster recovery

Hourly snapshots are taken by the `backup` Docker service using `pg_dump --format=custom --compress=9`. The last 24 snapshots are retained (24h coverage, RPO = 1h). Snapshots are stored in the `snapshots` Docker volume.

```bash
# immediate manual snapshot
docker compose exec backup /backup.sh

# list snapshots
docker compose exec backup ls -lht /snapshots/

# restore from latest snapshot
docker compose exec backup /restore.sh

# restore from a specific snapshot
docker compose exec backup /restore.sh /snapshots/reconciler_20260528T102712Z.dump

# tail backup log
docker compose exec backup tail -f /snapshots/backup.log
```

For production: mount the `snapshots` volume to durable cross-region object storage (S3, GCS) using a volume plugin or a sidecar that syncs dumps after each run.

---

## What I would change with another week

### 1. Authentication — JWT middleware

All endpoints are currently unauthenticated. The addition:

- JWT verification middleware inserted before the rate limiter (reject at the edge, cheapest possible)
- Short-lived access tokens (15 min) with `sub` (user ID) and `scope` claims
- Admin endpoints require a separate `admin` scope; user endpoints validate `sub` matches the path `{id}`
- JWKS endpoint for public key rotation without downtime
- Refresh token flow with a `refresh_tokens` table and rotation on use

### 2. Database — read replicas

A single Postgres instance handles reads and writes. With a replica:

- `GET /users/{id}/entitlement` routes to the replica (accepts ~100ms replication lag)
- Webhook ingestion and job claiming stay on the primary
- `pgx` connection pool split: `primary_dsn` for writes, `replica_dsn` for reads
- This isolates read traffic spikes from write throughput on the primary

### 3. Proper PITR and multi-region DR

The current `pg_dump` approach has a 1h RPO. A production DR plan:

```
Primary region (us-east-1)              DR region (eu-west-1)
──────────────────────────              ─────────────────────
  Postgres primary                        Postgres hot standby
       │  streaming replication ─────────▶  (seconds RPO)
       │
       │  WAL archive ──────────────────▶  S3 cross-region
       │  (continuous)                      (point-in-time recovery)
       │
  Patroni/etcd                           auto-promotes on primary failure
  (leader election)
```

- **Streaming replication** gives RPO in seconds and allows the standby to serve reads
- **WAL archiving** to cross-region object storage enables point-in-time restore beyond the standby's retention
- **Patroni** automates leader election and fencing to avoid split-brain
- Failover is a DSN update — no code change required

### 4. Multi-region active-active

The hard problem is write coordination. A pragmatic approach for entitlement data:

```
Region A (writes + reads)          Region B (reads + async)
──────────────────────────         ────────────────────────
  All webhook ingestion here         Entitlement reads served locally
  Carrier polling here               ~100ms replication lag acceptable
       │
       │  cross-region replication ──▶ read replica
```

Users are hashed to a home region. Carrier polling and notification scheduling use a distributed lock (Postgres advisory lock on the primary, or etcd) to prevent double-processing across regions. A per-user shard key avoids hot partitions.

### 5. Distributed tracing and structured logging

Current state: plain `log.Printf` to stdout.

The upgrade path:

- Replace with `slog` (Go 1.21+) for structured JSON logs with consistent field names (`user_id`, `event_id`, `source`, `duration_ms`, `trace_id`)
- Propagate `W3C Trace-Context` headers through the middleware chain
- Create spans around DB queries and external calls (carrier API)
- Export traces to Tempo (already in the Grafana stack) via OTLP
- Ship logs to Loki for cross-service correlation by `trace_id`

This makes it possible to answer "why did this user's entitlement change at 3am" by following a single trace ID from the inbound webhook through the service layer to every DB query it touched.
