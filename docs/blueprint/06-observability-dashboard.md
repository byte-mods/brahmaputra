# Blueprint 06 — Metrics Pipeline, Dashboard & Access Management

> Status: written from DESIGN.md §9 ahead of the M6 implementation;
> reconciled against the actual `metrics`/`dashboard` crates at the end of
> M6 (see the Verification section).

This document explains how operational data flows from code instrumentation
to a chart on a logged-in user's screen, and how access is controlled.

---

## 1. Instrumentation layer

All crates record through the `metrics` facade (`counter!`, `gauge!`,
`histogram!`) — zero cost to swap backends. Key metric families:

| Family | Examples |
|---|---|
| Throughput | `produce_requests_total`, `bytes_in_total`, `fetch_requests_total`, `bytes_out_total` (labeled by topic) |
| Latency | `produce_latency_ms`, `fetch_latency_ms` histograms (p50/p99/p999) |
| Log state | `log_end_offset`, `high_watermark`, `log_start_offset`, `log_size_bytes` per partition |
| Replication | `under_replicated_partitions`, `isr_shrinks_total`, `isr_expands_total`, `follower_lag_offsets` |
| Consumer groups | `group_lag_offsets`, `rebalances_total`, `group_members` |
| Control plane | `raft_leader`, `raft_commit_index`, `active_controller`, `metadata_cache_offset` |
| System | `request_queue_depth`, `connection_count`, `auth_failures_total` |

The partition actor and connection tasks update metrics inline; overhead is
a few atomic increments per batch — negligible against disk/network.

## 2. In-process time-series store

The dashboard needs charts without requiring Prometheus:

- A **registry** collects facade events into named series.
- Each series is a **ring buffer**: 5 s granularity × 4320 slots ≈ 6 h of
  history, fixed memory (~few MB for all series).
- Rollups (rate, p99) are computed at ingestion, so the API just reads
  arrays — no query engine, no dependencies.

`GET /metrics` separately renders the whole registry in Prometheus text
format (`metrics-exporter-prometheus`) for production monitoring stacks.

## 3. HTTP API (axum, embedded in every broker)

Routes per DESIGN.md §9.2. Read endpoints serve from the broker's local
metadata cache + metrics registry — never touching the controller, so
dashboard traffic cannot interfere with cluster management. Admin
endpoints (topic create/delete, user management) proxy to the active
controller.

Cluster-wide views (all brokers' metrics on one screen) are assembled by
**fan-out**: the serving broker calls its peers' `/api/v1/metrics/snapshot`
and merges. Simple, no central metrics service.

## 4. Dashboard SPA

- Vanilla JS + vendored Chart.js, embedded in the binary with `rust-embed`
  — no Node.js toolchain, one static binary serves everything.
- Pages: Login · Overview (brokers, topics, URP, throughput sparklines) ·
  Brokers · Topic detail (partitions: leader/ISR/LEO/HW/size) · Consumer
  groups (state, members, per-partition lag bars) · Metrics explorer
  (time-series charts from `/metrics/timeseries`) · Admin → Users.
- Polls every 5 s with the bearer token; renders from JSON only — all
  logic server-side.

## 5. Access management

```
login:  POST /api/v1/auth/login {username, password}
        → broker looks up UserRecord in its metadata cache
        → argon2::verify(password, password_hash)
        → JWT (HS256, claims: sub, roles, exp=12h) signed with cluster secret
authz:  middleware on every /api/v1/** route:
        → validate JWT signature + expiry
        → role guard: viewer ⊂ operator ⊂ admin
```

- **UserRecord lives in the Raft metadata log** (Blueprint 03 §1) → users
  are consistent cluster-wide; any broker can authenticate locally.
- **Roles**: `viewer` = read APIs; `operator` = + topic admin; `admin` =
  + user management. Data-plane produce/fetch ACLs reuse this user store in
  a later milestone.
- **Bootstrap**: first controller boot creates `admin` with password from
  `BRAHMAPUTRA_ADMIN_PASSWORD` (else generated, printed once to the log,
  forced change on first login).
- **Cluster JWT secret**: generated on first boot, stored as a metadata
  record, rotatable by admin (rotation invalidates all sessions — by
  design).
- Passwords: argon2id, per-user salt. Login endpoint is rate-limited;
  failures increment `auth_failures_total`.

## 6. Threat model (what this does and doesn't cover)

- Covers: unauthorized dashboard/API access, credential storage, token
  theft mitigation (short expiry), privilege separation for ops staff.
- Not yet: TLS on HTTP + data plane (M5 hardening — until then, deploy
  behind a reverse proxy / private network), data-plane ACLs, audit log.

---

## Verification (M6)

- [ ] Fresh cluster: login as bootstrapped admin works; wrong password
      rejected; unauthenticated API calls → 401.
- [ ] Create `viewer` and `operator` users via API; viewer gets 403 on
      topic create; operator gets 403 on user create; admin can do both.
- [ ] Produce/consume traffic → charts move on the dashboard; lag page
      matches `cli consume` offsets; `/metrics` scrapes in Prometheus
      format.
- [ ] Kill the broker serving the dashboard → same login works on any
      other broker (users replicated via Raft).
