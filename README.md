# DeadLockIQ: Distributed Concurrency & Deadlock Resolution Platform

> **DETECT → EXPLAIN → RESOLVE → RETRY → PROVE**

DeadLockIQ is a distributed concurrency and deadlock-recovery demonstration. It tracks transaction dependencies in a Wait-For Graph (WFG), detects cycles when lock waits are recorded, selects a victim using an explainable score, and demonstrates rollback and retry behavior. Redis and PostgreSQL integrations are optional at startup; when Redis is unavailable, lock operations use an in-memory fallback. The fallback is not a distributed lock service.

---

## 1. Architectural Highlights

- **Redis Lock Engine**: Lease-based locking with TTLs, fencing tokens, and atomic Redis operations when Redis is connected. An in-memory fallback supports local demonstrations but does not coordinate multiple application processes.
- **Monotonic Fencing Tokens**: 64-bit monotonically increasing sequence numbers issued with every lease grant. Stale workers delayed by GC or network partitions are blocked from mutating protected resources.
- **In-Memory Wait-For Graph (WFG)**: High-speed directed dependency graph tracking active locks and wait edges.
- **Incremental Cycle Detection**: Incremental reachability algorithm triggered on edge addition (`TxA -> TxB`) checking if `TxB` can reach `TxA`. Achieves sub-millisecond detection without full O(V+E) graph scans on uncontended locks.
- **Explainable Multi-Factor Victim Selection**:
  $$\text{Score} = (\text{Priority}_{\text{eff}} \times 5.0) + (|\text{Locks}| \times 4.0) + (\text{Work} \times 2.0) + (\text{Retries} \times 15.0) - (\text{RollbackCost} \times 1.5)$$
  Dynamic priority aging prevents starvation. The transaction with the lowest survival score is chosen, and a natural language data-driven explanation is generated comparing all candidates.
- **Automated Rollback & Jittered Retry Engine**:
  $$\text{Delay} = \min(\text{MaxDelay}, \text{BaseDelay} \times 2^{\text{RetryCount}}) + \text{Jitter}$$
  Jitter can spread retries under contention; progress depends on the lock and transaction state being released correctly.
- **PostgreSQL Lock Observation**: Polls `pg_locks` and `pg_stat_activity` when PostgreSQL is available. The `POSTGRES_DEADLOCK` demo is simulated and does not prove live PostgreSQL deadlock detection.
- **Live Reactive Dashboard**: Interactive React Flow visualizer with animated dependency edges, pulsing deadlock cycle highlights, live metrics, and real-time streaming WebSocket event logs.

---

## 2. Scenario Catalog

1. **Two-Way Deadlock**: Classic circular wait between 2 transactions on 2 resources.
2. **Three-Way Deadlock**: Triangular circular wait ring (`Tx1 -> ResB -> Tx2 -> ResC -> Tx3 -> ResA -> Tx1`).
3. **N-Way Deadlock**: 5-worker circular dependency ring.
4. **Normal Contention**: Queues on a shared resource without creating a deadlock cycle.
5. **Multiple Deadlocks**: Creates two independent two-transaction cycles.
6. **Stale Worker**: Expires a lease and demonstrates rejection of an older fencing token.
7. **Fencing Violation**: Runs the stale-worker fencing demonstration.
8. **Retry Storm**: Exercises concurrent workers contending for one resource and jittered retry behavior.
9. **PostgreSQL Deadlock**: Seeds simulated PostgreSQL lock records; this is not a live database deadlock test.
10. **Redis Failure**: Injects a simulated Redis failure. This is separate from testing a real Redis outage.
11. **Worker Crash**: Lets a lease expire after its owner stops releasing it.
12. **Network Delay**: Injects synthetic latency into lock operations.
13. **Cross-System Deadlock**: Requires a reachable PostgreSQL database and Redis/in-memory lock coordination. It is not fully exercised when PostgreSQL is unavailable.

---

## 3. Quick Start

### Docker Compose

The Compose file can start the supporting services without building the backend:

```bash
docker compose up -d redis postgres prometheus grafana
```

The Compose configuration maps Redis and PostgreSQL to host ports `6379` and `5432`. The backend service is configured to use `redis:6379` and `postgres:5432` on the Compose network and to wait for healthy dependencies. However, this workspace does not contain the root `Dockerfile` referenced by the backend service, so a full `docker compose up -d --build` cannot build the application container yet. Run the application locally with the Node instructions below, or add the missing Dockerfile before using the containerized backend.

- Application: `http://localhost:3000`
- Redis: `localhost:6379`
- PostgreSQL: `localhost:5432`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3001`

Check service state with `docker compose ps` and logs with `docker compose logs redis postgres`. Once a backend Dockerfile is available, build and recreate it with `docker compose up -d --build`.

### Running the Node/TypeScript Application

```bash
npm install
npm run dev
```

The application listens on port `3000` and exposes WebSockets at `/ws`.

To connect to services outside Docker, set environment variables before starting the application:

```powershell
$env:REDIS_URL = 'rediss://default:<UPSTASH_TOKEN>@<UPSTASH_HOST>:6379'
$env:REDIS_TLS = 'true'
$env:DATABASE_URL = 'postgres://deadlockiq:deadlockiq_secret@localhost:5432/deadlockiq_production?sslmode=disable'
npm run dev
```

Replace the Upstash placeholders with values from your Upstash dashboard; do not commit credentials. A `rediss://` URL automatically enables TLS. When using Docker Compose, its backend service forwards the `REDIS_URL` and `REDIS_TLS` environment variables; set them in the shell before running Compose. `DATABASE_URL` must also match a reachable PostgreSQL service.

When Redis cannot be reached, the engine reports `OFFLINE` and uses an in-memory lock fallback. When PostgreSQL cannot be reached, the observer reports `OFFLINE`; PostgreSQL-dependent scenarios cannot be considered live integration tests.

---

## 4. REST API Reference

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/v1/health` | Service health, Redis status, and worker count |
| `GET` | `/metrics` | Prometheus OpenMetrics exporter endpoint |
| `GET` | `/api/v1/metrics` | JSON snapshot of P50, P95, P99 latencies & counters |
| `GET` | `/api/v1/transactions` | List all active and completed transactions |
| `POST` | `/api/v1/transactions` | Register a new transaction with priority |
| `POST` | `/api/v1/locks/acquire` | Acquire distributed lock with lease TTL |
| `POST` | `/api/v1/locks/release` | Atomically release lock verifying fencing token |
| `GET` | `/api/v1/graph` | Wait-For Graph snapshot formatted for React Flow |
| `GET` | `/api/v1/deadlocks` | List all recorded deadlock incidents |
| `POST` | `/api/v1/simulator/scenarios/:name` | Run one of the 13 scenarios |
| `POST` | `/api/v1/simulator/reset` | Reset all engine state, locks, and graphs |
| `POST` | `/api/v1/benchmark/run` | Run the concurrency benchmark |

Scenario names are uppercase identifiers, for example `TWO_WAY_DEADLOCK` and `NORMAL_CONTENTION`. Reset between independent scenario runs:

```powershell
Invoke-RestMethod -Method Post http://localhost:3000/api/v1/simulator/reset
Invoke-RestMethod -Method Post http://localhost:3000/api/v1/simulator/scenarios/TWO_WAY_DEADLOCK
```

Check the response body, transaction states, graph, incidents, and metrics after each run. HTTP success alone does not prove recovery completed.

Useful inspection endpoints include `/api/v1/health`, `/api/v1/transactions`, `/api/v1/graph`, `/api/v1/deadlocks`, `/api/v1/metrics`, and `/api/v1/postgres/locks`.

---

## 5. Go Backend Reference Implementation

The Go reference codebase is located in `backend/`:
- `backend/internal/fencing/`: Monotonic 64-bit atomic sequence generator.
- `backend/internal/graph/`: Concurrency-safe Wait-For Graph using `sync.RWMutex` with BFS incremental reachability.
- `backend/tests/`: Unit tests with `-race` support (`go test -race ./...`).

---

## 6. Verification

Run frontend checks from the repository root:

```powershell
npx tsc --noEmit
npm run build
```

Run Go checks from `backend/`:

```powershell
go test ./...
go test -race ./...
go build ./...
```

These commands verify compilation and tests, not live Redis/PostgreSQL connectivity or browser scenario recovery. Record measured performance only from a fresh benchmark run in the target environment; no fixed performance figures are claimed here.
