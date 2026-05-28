# Subscription Reconciler

A subscription entitlement reconciliation service that aggregates access signals from multiple sources (App Store, Carrier, Marketplace) into a single canonical entitlement state per user. Built in Go with PostgreSQL as the sole runtime dependency.

---

## Table of Contents

- [Running the project](#running-the-project)
- [API reference](#api-reference)
- [High-level design](#high-level-design)
- [Data flow](#data-flow)
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

| Service    | URL                                   |
|------------|---------------------------------------|
| API        | http://localhost:8080                 |
| Grafana    | http://localhost:3000 (admin / admin) |
| Prometheus | http://localhost:9090                 |
| Postgres   | localhost:5432                        |

**Viewing database tables:**

```bash
# list all tables
docker compose exec postgres psql -U reconciler -d reconciler -c "\dt"

# query a table
docker compose exec postgres psql -U reconciler -d reconciler -c "SELECT * FROM source_entitlements LIMIT 20;"

# interactive shell (\dt = list tables, \d <table> = schema, \q = quit)
docker compose exec postgres psql -U reconciler -d reconciler
```

Or connect any GUI client (TablePlus, DBeaver, pgAdmin) with:

| Field    | Value       |
|----------|-------------|
| Host     | `localhost` |
| Port     | `5432`      |
| User     | `reconciler`|
| Password | `reconciler`|
| Database | `reconciler`|

The Grafana **Entitlement Timeline & Audit Log** dashboard at http://localhost:3000 also renders `source_entitlements` and `entitlement_audit_log` as live tables.

**Running tests** (requires a live Postgres):

```bash
TEST_DATABASE_URL="postgres://reconciler:reconciler@localhost:5432/reconciler?sslmode=disable" \
  go test ./internal/service/... -v
```

---

## API reference

### `GET /users/{id}/entitlement`

Returns the canonical current entitlement state for a user. Source priority: `STORE > CARRIER > MARKETPLACE > NONE`.

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

No active entitlement → `{ "active": false, "source": "NONE", ... }`

---

### `GET /users/{id}/timeline`

Returns the full audit history of entitlement state transitions for a user, newest first.

```bash
# JSON
curl http://localhost:8080/users/u_42/timeline

# CSV download
curl "http://localhost:8080/users/u_42/timeline?format=csv" -o timeline.csv
```

```json
{
  "userId": "u_42",
  "timeline": [
    {
      "id": 2,
      "source": "STORE",
      "eventId": "evt_cancel_1",
      "prev": { "active": true, "reason": "INITIAL_PURCHASE", "expiresAt": "2026-06-27T10:00:00Z" },
      "next": { "active": true, "reason": "CANCELLATION",     "expiresAt": "2026-05-29T10:00:00Z" },
      "changedAt": "2026-05-28T11:07:36Z"
    },
    {
      "id": 1,
      "source": "STORE",
      "eventId": "evt_purchase_1",
      "prev": null,
      "next": { "active": true, "reason": "INITIAL_PURCHASE", "expiresAt": "2026-06-27T10:00:00Z" },
      "changedAt": "2026-05-28T10:00:00Z"
    }
  ]
}
```

CSV columns: `id, source, eventId, prevActive, prevReason, prevExpiresAt, nextActive, nextReason, nextExpiresAt, changedAt`

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

| Event type       | active | expiresAt   | Notification |
|------------------|--------|-------------|--------------|
| INITIAL_PURCHASE | true   | +30 days    | —            |
| RENEWAL          | true   | +30 days    | —            |
| UN_CANCELLATION  | true   | +30 days    | —            |
| CANCELLATION     | true   | +24h grace  | if ≤24h away |
| BILLING_ISSUE    | true   | +24h grace  | if ≤24h away |
| EXPIRATION       | false  | —           | —            |

Events are deduplicated by `eventId`. Out-of-order delivery is handled by `eventTimeMs` — a late-arriving older event never overrides a newer state. Every applied event writes an entry to `entitlement_audit_log`.

---

### `POST /webhooks/marketplace/revoke`

Bulk-revokes marketplace entitlements.

```bash
curl -X POST http://localhost:8080/webhooks/marketplace/revoke \
  -H "Content-Type: application/json" \
  -d '{"userIds": ["u_42", "u_99"]}'
```

---

### `GET /admin/config` · `POST /admin/config`

Live runtime configuration — no restart required.

```bash
curl http://localhost:8080/admin/config

curl -X POST http://localhost:8080/admin/config \
  -H "Content-Type: application/json" \
  -d '{"key": "api_rate_limit_per_minute", "value": "60"}'
```

| Key                             | Default | Description                         |
|---------------------------------|---------|-------------------------------------|
| `api_rate_limit_per_minute`     | `120`   | Max requests per IP per minute      |
| `api_request_cache_ttl_seconds` | `600`   | Idempotency window in seconds       |
| `api_gateway_enabled`           | `true`  | Toggle the request cache middleware |

---

## High-level design

```
╔══════════════════════════════════════════════════════════════════════════════╗
║                             CLIENT REQUESTS                                  ║
║          Webhooks (Store · Marketplace)  ·  Reads  ·  Timeline              ║
╚══════════════════════════╦═══════════════════════════════════════════════════╝
                           ║
                           ▼
╔══════════════════════════════════════════════════════════════════════════════╗
║                          MIDDLEWARE CHAIN                                    ║
║                                                                              ║
║  ┌──────────────────────────────────────────────────────────────────────┐   ║
║  │  1  RATE LIMITER                                                      │   ║
║  │     Postgres-backed sliding window · per IP × method × path          │   ║
║  │     Default 120 req/min · live-tunable via app_config                │   ║
║  │     → 429 Too Many Requests when exceeded                            │   ║
║  └────────────────────────────────┬─────────────────────────────────────┘   ║
║                                   │                                          ║
║  ┌────────────────────────────────▼─────────────────────────────────────┐   ║
║  │  2  QUERY SUPPRESSOR  (Postgres Idempotency Cache)                    │   ║
║  │     POST / PUT / PATCH / DELETE only                                  │   ║
║  │     Key: Idempotency-Key header  OR  SHA-256(request body)           │   ║
║  │     ┌──────────┐  ┌────────────────────┐  ┌──────────────────────┐  │   ║
║  │     │ 1st req  │  │ duplicate in TTL   │  │ concurrent duplicate  │  │   ║
║  │     │ CLAIMED  │  │ response replayed  │  │ 202 in_progress      │  │   ║
║  │     └──────────┘  └────────────────────┘  └──────────────────────┘  │   ║
║  └────────────────────────────────┬─────────────────────────────────────┘   ║
║                                   │                                          ║
║  ┌────────────────────────────────▼─────────────────────────────────────┐   ║
║  │  3  INSTRUMENTATION                                                   │   ║
║  │     Latency histograms (p50/p95/p99) · status code counters          │   ║
║  │     → Prometheus scrape at GET /metrics every 10s                    │   ║
║  └────────────────────────────────┬─────────────────────────────────────┘   ║
╚══════════════════════════════════╦══════════════════════════════════════════╝
                                   ║
                                   ▼
╔══════════════════════════════════════════════════════════════════════════════╗
║                            HTTP HANDLERS                                     ║
║                                                                              ║
║   POST /webhooks/store              POST /webhooks/marketplace/revoke        ║
║   GET  /users/{id}/entitlement      GET  /users/{id}/timeline                ║
║   GET|POST /admin/config            GET  /healthz  ·  GET /metrics           ║
╚══════════════════════════╦═══════════════════════════════════════════════════╝
                           ║
                           ▼
╔══════════════════════════════════════════════════════════════════════════════╗
║                         ENTITLEMENT SERVICE                                  ║
║                                                                              ║
║   Priority      STORE  >  CARRIER  >  MARKETPLACE  >  NONE                  ║
║   Ordering      eventTimeMs — stale events rejected, never override newer    ║
║   Dedup         In-memory FIFO (10K) fast path before DB event_id PK check  ║
║   Mutations     All writes in explicit DB transactions                       ║
║   Audit         Every upsert atomically writes entitlement_audit_log via CTE ║
╚══╦═══════════════════════════════════╦════════════════════════════════════╦══╝
   ║                                   ║                                    ║
   ▼                                   ▼                                    ▼
╔══════════════════╗   ╔═══════════════════════════════╗   ╔════════════════════╗
║   POSTGRESQL     ║   ║    BACKGROUND WORKERS         ║   ║   OBSERVABILITY   ║
║                  ║   ║                               ║   ║                   ║
║  store_events    ║   ║  ┌─────────────────────────┐  ║   ║  Prometheus :9090 ║
║  source_entitle  ║   ║  │  CARRIER POLLER          │  ║   ║  Grafana    :3000 ║
║  ments           ║◀══╣  │  every 5 min             │  ║   ║                   ║
║  carrier_poll_   ║   ║  │  pool of 8 goroutines    │  ║   ║  Dashboards:      ║
║  jobs            ║   ║  │  FOR UPDATE SKIP LOCKED  │  ║   ║  · API metrics    ║
║  notifications   ║   ║  └─────────────────────────┘  ║   ║  · Audit timeline ║
║  entitlement_    ║   ║                               ║   ║  · DB tables      ║
║  audit_log       ║   ║  ┌─────────────────────────┐  ║   ║                   ║
║  api_request_    ║   ║  │  NOTIFICATION SENDER     │  ║   ║  Postgres data    ║
║  cache           ║◀══╣  │  every 30s               │  ║   ║  source for live  ║
║  api_rate_limits ║   ║  │  marks sent_at           │  ║   ║  table queries    ║
║  app_config      ║   ║  │  FOR UPDATE SKIP LOCKED  │  ║   ╚════════════════════╝
╚══════════╦═══════╝   ║  └─────────────────────────┘  ║
           ║           ╚═══════════════════════════════╝
           ▼
╔══════════════════════════════════════════════╗
║              BACKUP SERVICE                  ║
║                                              ║
║  pg_dump --format=custom --compress=9        ║
║  runs every hour via crond                   ║
║  retains last 24 snapshots (24h coverage)    ║
║  RPO = 1h  ·  restore: pg_restore --clean   ║
╚══════════════════╦═══════════════════════════╝
                   ║
                   ▼
╔══════════════════════════════════════════════╗
║           snapshots Docker volume            ║
║   (mount to S3/GCS for production DR)        ║
╚══════════════════════════════════════════════╝
```

---

## Data flow

### Store webhook → entitlement → audit → notification

```
  Client
    │
    │  POST /webhooks/store  {eventId, userId, type, eventTimeMs, productId}
    │
    ▼
  In-memory seenCache (10K FIFO)
    │  eventId already seen? ──yes──▶  200 {status: "duplicate"}
    │  no
    ▼
  DB transaction begins
    │
    ├─▶  INSERT store_events (eventId PK)
    │       conflict? ──yes──▶  commit, return applied=false
    │
    ├─▶  SELECT last_event_time_ms for (userId, STORE)
    │       event older than stored? ──yes──▶  commit, skip update
    │
    ├─▶  storeTransition(event)  →  (active bool, expiresAt *time.Time)
    │
    ├─▶  WITH prev AS (SELECT current row)
    │    upserted AS (INSERT ... ON CONFLICT DO UPDATE RETURNING ...)
    │    INSERT INTO entitlement_audit_log          ← atomic, same CTE
    │         prev_active/reason/expiresAt = old row
    │         next_active/reason/expiresAt = new row
    │
    └─▶  if active && expiresAt within 24h:
             INSERT INTO notifications ON CONFLICT DO NOTHING   ← idempotent
    │
  commit
    │
    ▼
  200 {status: "ok", applied: true}


  (every 30s) NotificationSender
    │
    ├─▶  UPDATE notifications SET sent_at = now()
    │    WHERE sent_at IS NULL AND scheduled_for <= now()
    │    FOR UPDATE SKIP LOCKED
    │
    └─▶  Prometheus counter incremented
```

### Carrier entitlement flow

```
  (every 5 min) CarrierPoller
    │
    ├─▶  ClaimCarrierPollJobs  (FOR UPDATE SKIP LOCKED, batch of 100)
    │
    └─▶  for each userId (pool of 8 goroutines):
           GET /mock/carrier/plan?userId=...
             │
             ├── "active"    → UpsertSourceEntitlement(CARRIER, active=true)
             │                  + ScheduleCarrierPoll in 5 min
             │                  + audit log entry
             │
             ├── "inactive"  → UpsertSourceEntitlement(CARRIER, active=false)
             │                  + DELETE carrier_poll_jobs
             │                  + audit log entry
             │
             └── "api_error" → ScheduleCarrierPoll retry in 5 min
                                no entitlement change, no audit entry
```

### Timeline read

```
  Client
    │
    │  GET /users/{id}/timeline[?format=csv]
    ▼
  SELECT * FROM entitlement_audit_log
  WHERE user_id = $1
  ORDER BY created_at DESC
    │
    ├── format=csv  →  Content-Disposition: attachment; filename="timeline_{id}.csv"
    │                  CSV rows: id,source,eventId,prevActive,...,changedAt
    │
    └── (default)   →  JSON  {userId, timeline: [{id, source, eventId, prev, next, changedAt}]}
```

---

## Component deep-dives

### Entitlement model

Each user has at most one row per source in `source_entitlements`. The read path queries all active rows ordered by source priority and returns the highest-priority one as the canonical state.

```
user_id │ source      │ active │ reason          │ expires_at
────────┼─────────────┼────────┼─────────────────┼────────────
u_42    │ STORE       │ true   │ RENEWAL         │ 2026-06-27
u_42    │ CARRIER     │ true   │ CARRIER_ACTIVE  │ null
u_42    │ MARKETPLACE │ false  │ MP_REVOKE       │ null

→ GET /users/u_42/entitlement  →  source: STORE  (priority wins)
→ GET /users/u_42/timeline     →  full history of all 3 source rows
```

### Audit log

Every call to `UpsertSourceEntitlementTx` atomically captures the transition via a CTE — no separate round-trip:

```sql
WITH prev AS (
    SELECT active, reason, expires_at FROM source_entitlements
    WHERE user_id = $1 AND source = $2
),
upserted AS (
    INSERT INTO source_entitlements ... ON CONFLICT DO UPDATE ...
    RETURNING active, reason, expires_at
)
INSERT INTO entitlement_audit_log (
    user_id, source, event_id,
    prev_active, prev_reason, prev_expires_at,   -- null on first write
    next_active, next_reason, next_expires_at
)
SELECT $1, $2, NULLIF($8,''), prev.*, upserted.*
FROM upserted LEFT JOIN prev ON true
```

`prev` is `null` on the first write for a (user, source) pair. Store webhooks carry `eventId`; carrier and marketplace transitions set it to `null`.

### Rate limiter

A single `INSERT ... ON CONFLICT DO UPDATE` atomically increments the counter or resets it when the window expires. No cleanup job needed — stale rows are overwritten on the next request. The limit is read from `app_config` on every request for live tunability.

### Query suppressor (Postgres idempotency cache)

| Scenario | Behaviour |
|---|---|
| First arrival | Insert `PROCESSING`, run handler, store response as `COMPLETED` |
| Duplicate within TTL | Replay cached response |
| Concurrent duplicate | `state=PROCESSING` → 202 `duplicate_in_progress` |
| Body mismatch on same key | 409 `conflict` |

Keys are either the `Idempotency-Key` header or SHA-256 of the body. State lives in Postgres in the same pool as business data — no cache coherence problem.

### Expiry notification pipeline

```
StoreWebhook ingested
  └─▶ storeTransition() → (active, expiresAt)
        └─▶ if 0 < time.Until(expiresAt) ≤ 24h:
              INSERT INTO notifications
              ON CONFLICT (user_id, entitlement_source, expires_at) DO NOTHING
                                         ↑ at-most-once guarantee

Every 30s — NotificationSender:
  UPDATE notifications SET sent_at = now()
  WHERE sent_at IS NULL AND scheduled_for <= now()
  FOR UPDATE SKIP LOCKED
```

---

## Design decisions

**Single datastore (PostgreSQL).** Rate limiting, idempotency, job queues, audit log, and notifications all live in Postgres. No Redis, no Kafka. At this scale the connection pool is the only shared constraint; all features degrade gracefully when Postgres is slow rather than failing independently.

**Atomic audit via CTE.** The audit entry is written in the same SQL statement as the upsert using a CTE with `RETURNING`. This guarantees the audit log is never missing a transition and never duplicates one — no application-level coordination needed.

**Event ordering by `eventTimeMs`.** Store webhooks arrive out of order. Comparing `eventTimeMs` to `last_event_time_ms` before applying ensures the DB always reflects the most recent logical state, not the most recently received packet.

**Source priority at read time.** Each source owns its own row; the winner is selected by an `ORDER BY CASE source` at query time. Partial revocations (e.g., marketplace-only) are trivially correct. The alternative — a merged row updated on every source change — creates race conditions and destroys the per-source audit trail.

**Pre-warmed in-memory cache for webhook dedup.** On startup the service loads the last 10,000 `event_id`s from `store_events` into a bounded LRU map. Subsequent duplicates are rejected in memory before a DB transaction is opened. Correctness is guaranteed by the `store_events.event_id PRIMARY KEY` (`ON CONFLICT DO NOTHING`) — the cache is a performance optimisation only. When the DB catches a cross-replica or post-eviction duplicate it back-fills the cache so the next retry is free. At higher scale this layer would be replaced by a shared Redis bloom filter, but that would add an external dependency and an extra network hop; the pre-warm approach eliminates both while covering the hot-retry window that matters in practice.

**Live config via `app_config`.** Rate limit and cache TTL are read on every request, allowing operators to tune without a deploy. The round-trip cost is negligible compared to the main business query.

---

## Tradeoffs considered

| Decision | Alternative | Why this choice |
|---|---|---|
| Postgres for rate limiting | Redis | No extra service; sliding window sufficient at this scale |
| Postgres for idempotency | Redis + Lua | Transactional consistency; no cache invalidation problem |
| CTE for atomic audit | App-level read-then-write | Eliminates TOCTOU race; one round-trip instead of two |
| `FOR UPDATE SKIP LOCKED` queues | SQS, RabbitMQ | No external dependencies; Postgres already trusted |
| `pg_dump` snapshots | WAL/PITR, pgBackRest | Simpler to operate; portable; 1h RPO acceptable at this scale |
| Custom Prometheus exporter | `prometheus/client_golang` | Zero external dependencies; full control over label names |
| Source priority at read time | Materialised merged row | Simpler partial revocations; no concurrent update races |
| Pre-warmed in-memory LRU for event dedup | Redis bloom filter | No extra service; DB pre-warm on startup covers post-restart duplicates; DB `ON CONFLICT` is authoritative |

---

## Observability

Metrics at `GET /metrics` (Prometheus text), scraped every 10s, visualised in Grafana.

| Metric | Type | Description |
|---|---|---|
| `subscription_api_requests_total` | counter | Requests by method, path, status |
| `subscription_api_request_duration_seconds` | histogram | Latency — buckets 5ms → 10s |
| `subscription_api_rate_limiter_total` | counter | allowed / blocked / error |
| `subscription_api_request_cache_total` | counter | claimed / replay / in_progress / body_mismatch |
| `subscription_carrier_poll_total` | counter | active / inactive / api_error / apply_error |
| `subscription_notifications_sent_total` | counter | Notifications stamped sent |
| `subscription_carrier_last_batch_size` | gauge | Last carrier poll batch size |
| `go_goroutines` | gauge | Live goroutine count |
| `go_memstats_alloc_bytes` | gauge | Heap allocation |
| `process_cpu_seconds_total` | counter | Total CPU time |

**Grafana dashboards** (http://localhost:3000 — admin / admin):

| Dashboard | Data source | Key panels |
|---|---|---|
| Subscription Reconciler | Prometheus | Request rate, p95 latency, cache decisions, carrier polls, notifications |
| Entitlement Timeline & Audit Log | Postgres | Recent audit log, transitions over time, revocations, active users by source, current entitlements table |

**Subscription Reconciler** (Prometheus metrics):

![Subscription Reconciler Dashboard](observability/screenshots/dashboard-metrics.png)

**Entitlement Timeline & Audit Log** (Postgres data):

![Entitlement Timeline & Audit Log Dashboard](observability/screenshots/dashboard-audit-log.png)

---

## Disaster recovery

Hourly snapshots via the `backup` Docker service. Last 24 retained → 24h coverage, RPO = 1h.

```bash
# immediate snapshot
docker compose exec backup /backup.sh

# list snapshots
docker compose exec backup ls -lht /snapshots/

# restore from latest
docker compose exec backup /restore.sh

# restore specific snapshot
docker compose exec backup /restore.sh /snapshots/reconciler_20260528T102712Z.dump

# tail log
docker compose exec backup tail -f /snapshots/backup.log
```

For production: mount `snapshots` volume to S3/GCS via a sidecar that syncs after each run.

---

## What I would change with another week

### 1. JWT authentication

All endpoints are currently open. The addition:

- JWT verification middleware inserted **before the rate limiter** — reject at the edge, cheapest possible
- Short-lived access tokens (15 min) with `sub` and `scope` claims
- Admin endpoints require `scope=admin`; user endpoints validate `sub == path {id}`
- JWKS endpoint for key rotation without downtime

### 2. Read replicas

- `GET /users/{id}/entitlement` and `GET /users/{id}/timeline` route to the replica (~100ms lag acceptable)
- All writes and job claiming stay on the primary
- `pgx` pool split: `primary_dsn` writes, `replica_dsn` reads

### 3. PITR and multi-region DR

```
Primary (us-east-1)                   DR (eu-west-1)
─────────────────────                 ─────────────────────
  Postgres primary                      Postgres hot standby
      │  streaming replication ────────▶  RPO: seconds
      │  WAL archive ─────────────────▶  S3 cross-region (PITR)
  Patroni/etcd  ─────────────────────▶  auto-promote on failure
```

Failover is a DSN update — no code change.

### 4. Multi-region active-active

Users hashed to a home region. All writes for a user go to their home region's primary. Reads served from local replica. Carrier polling and notification scheduling guarded by distributed lock (etcd or Postgres advisory lock on the primary) to avoid double-processing.

### 5. Distributed tracing and structured logging

- Replace `log.Printf` with `slog` (structured JSON: `user_id`, `event_id`, `source`, `trace_id`, `duration_ms`)
- Propagate W3C Trace-Context headers through the middleware chain
- Spans around every DB query and external call
- Export to Tempo via OTLP; logs to Loki — correlate by `trace_id`

This makes "why did this user's entitlement change at 3am" answerable in under 30 seconds.
