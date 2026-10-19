# Election System Architecture & Write Compression

Distributed, high-throughput election tallying architecture built on `d-core-actor-system` and `d-core-async-proxy` (NATS JetStream + Valkey partition leases + PostgreSQL).

---

## 1. Executive Summary & Load Test Benchmark

Under load testing, traditional relational architectures buckle when handling election traffic: every vote requires verifying identity, writing a ballot row, updating precinct counters, and updating national totals. Under unbatched ingestion, 3,000–5,000 votes/s translates to **12,000+ SQL statements/s**, instantly overwhelming PostgreSQL connection pools and CPU quotas.

In the Election System benchmark, the actor-driven architecture achieved **over 20x sustained Database Write Compression** (`>20:1` logical votes persisted per executed SQL statement) while running against a strictly constrained PostgreSQL container (**0.75 CPU limit, 512 MB RAM, 16 connection pool**):

| Metric | Naive Relational Path | Optimized Actor Stack | Net Gain |
| :--- | :--- | :--- | :--- |
| **Ingress Throughput** | ~800–1,200 req/s (saturated) | **3,000–5,000+ req/s** | **4x–5x higher throughput** |
| **SQL Statements / Vote** | 2 to 4 statements / vote | **~0.04 to 0.05 statements / vote** | **20x to 25x reduction** |
| **DB Write Compression Ratio** | 1.0 (no compression) | **20x – 60x+** | **>20x compression** |
| **PostgreSQL CPU Throttling** | 41% – 49% CFS throttling | **< 18% (quiet)** | **Eliminated DB bottleneck** |
| **SQL Query Latency p95** | 584 ms (queueing tail) | **62 ms** | **~90% tail latency drop** |
| **Tally Read Latency** | 15–40 ms (DB aggregate lock contention) | **~0.1 ms (in-memory actor read)** | **300x faster reads** |

---

## 2. End-to-End Architectural Layout

```mermaid
flowchart TD
    subgraph Clients ["Ingress Clients & Load Generators"]
        K6["k6 Distributed Load Generators\n(3,000 - 5,000+ votes/s)"]
        Obs["Tally Observers\n(GET /elections/:id/tally)"]
    end

    subgraph APITier ["API & Gateway Tier"]
        NginxHTTP["Nginx HTTP Reverse Proxy\n(:8080)"]
        API1["election-api-backend #1"]
        API2["election-api-backend #2"]
        API3["election-api-backend #3"]
    end

    subgraph BrokerTier ["Decoupled Streaming & Lease Control Plane"]
        NginxGRPC["async-proxy-gateway\n(gRPC LB :50051 / :50052)"]
        Proxy1["async-proxy #1"]
        Proxy2["async-proxy #2"]
        Proxy3["async-proxy #3"]
        NATS["NATS JetStream 2.10\nStream: proxy_election_votes\n32 Partitions (p00 - p31)"]
        Valkey["Valkey 8 (Lease & Fencing)\n--appendonly yes --maxmemory-policy noeviction\nRendezvous Consistent Hashing"]
    end

    subgraph ActorTier ["Stateful In-Memory Actor Tier (3 Replicas)"]
        subgraph Actor1 ["election-actors Node 1 (p00 - p10)"]
            P1["Precinct Actors\n(In-memory :voted-ids O(1) dedup)\n(:pending-ballots buffer: 200)"]
        end
        subgraph Actor2 ["election-actors Node 2 (p11 - p21)"]
            P2["Precinct Actors\n(Multi-row flush)\n(Coalesced deltas: 200ms)"]
        end
        subgraph Actor3 ["election-actors Node 3 (p22 - p31)"]
            P3["Precinct Actors"]
            TallyActor["Parent :election-tally Actor\n(In-memory national state)\n(Flushes every 50 deltas)"]
        end
    end

    subgraph StorageTier ["Constrained PostgreSQL 16 (0.75 CPU, 16 Pool)"]
        DB_Ballots[("ballots table\n(Multi-Row batch inserts\n200 rows per SQL stmt)")]
        DB_Precinct[("precinct_tallies table\n(Periodic checkpoint upsert\nguarded by ownership_epoch)")]
        DB_Election[("election_tallies table\n(Aggregated summary upsert\nguarded by ownership_epoch)")]
    end

    %% Client paths
    K6 -->|POST /elections/:id/votes\n(HTTP 202 Accepted)| NginxHTTP
    Obs -->|GET /elections/:id/tally\n(0.1ms read)| NginxHTTP
    NginxHTTP --> API1 & API2 & API3

    %% API to Proxy
    API1 & API2 & API3 -->|gRPC Unary Publish\nproxy.client.publish| NginxGRPC
    NginxGRPC --> Proxy1 & Proxy2 & Proxy3
    Proxy1 & Proxy2 & Proxy3 -->|Enqueue to Durable Partitions| NATS

    %% Lease validation
    Proxy1 & Proxy2 & Proxy3 -.->|Validate Lease Token| Valkey
    Actor1 & Actor2 & Actor3 -.->|Acquire & Renew Leases\nFencing Token| Valkey

    %% Proxy to Actors
    NATS -->|gRPC Server Streaming\nEgress :50052| Actor1 & Actor2 & Actor3

    %% Actor internal messaging
    P1 & P2 & P3 -->|Coalesced :precinct-delta\n(at most every 200ms)| TallyActor

    %% Actor reads (Actor-First)
    API1 & API2 & API3 -.->|Direct In-Memory Peek\nactor.peek_state (~0.1ms)| TallyActor

    %% Database Writes
    P1 & P2 & P3 ==>|1. Multi-Row INSERT Batch\n(1 stmt / 200 ballots)| DB_Ballots
    P1 & P2 & P3 -.->|2. Periodic Checkpoint Upsert\n(Off hotpath: tick-max-age 10s)| DB_Precinct
    TallyActor ==>|3. Summary Upsert\n(1 stmt / 50 deltas)| DB_Election

    %% Styling
    classDef client fill:#1e293b,stroke:#475569,stroke-width:1px,color:#f8fafc;
    classDef api fill:#0f172a,stroke:#3b82f6,stroke-width:2px,color:#f8fafc;
    classDef broker fill:#0f172a,stroke:#10b981,stroke-width:2px,color:#f8fafc;
    classDef actor fill:#0f172a,stroke:#8b5cf6,stroke-width:2px,color:#f8fafc;
    classDef storage fill:#0f172a,stroke:#f59e0b,stroke-width:2px,color:#f8fafc;

    class K6,Obs client;
    class NginxHTTP,API1,API2,API3 api;
    class NginxGRPC,Proxy1,Proxy2,Proxy3,NATS,Valkey broker;
    class Actor1,Actor2,Actor3,P1,P2,P3,TallyActor actor;
    class DB_Ballots,DB_Precinct,DB_Election storage;
```

---

## 3. How the 20x+ DB Write Compression is Achieved

Write compression is not a single trick; it is an architectural pipeline where each layer collapses a different dimension of database traffic.

### Lever 1: O(1) In-Memory Deduplication (Eliminating Read-Before-Write)
* **The Problem:** Preventing duplicate voting traditionally requires a database query:
  ```sql
  SELECT 1 FROM ballots WHERE voter_id = 'voter-123' AND election_id = 'national-2026';
  ```
  At 3,000 votes/s, this generates **3,000 DB read queries/s** just to validate incoming traffic.
* **The Actor Solution:** Each precinct is modeled as an in-memory entity actor (`election-system.actors.precinct`) with single-threaded mailbox serialization. The actor maintains a `:voted-ids` set in heap memory.
* **Result:** Duplicate checks execute in **$O(1)$** memory time. Rejected votes emit zero SQL statements. Valid votes proceed without any prior database roundtrip.

### Lever 2: Multi-Row Ballot Batching (`insert-batch!`)
* **The Problem:** Individual `INSERT INTO ballots (...) VALUES (...)` statements force constant query parsing, transaction setup, WAL syncs, and index traversal overhead.
* **The Actor Solution:** As votes arrive in the mailbox, they append to the `:pending-ballots` vector. When `count >= BATCH_SIZE (200)`, the actor triggers an atomic flush effect (`:db/flush-precinct`).
* **The SQL Execution:**
  ```sql
  INSERT INTO ballots (ballot_id, election_id, precinct_id, voter_id, candidate_id, cast_at)
  VALUES 
    ('b1', 'nat', 'p1', 'v1', 'alice', '...'),
    ('b2', 'nat', 'p1', 'v2', 'bob', '...'),
    ... -- 200 rows in ONE statement
  ON CONFLICT DO NOTHING;
  ```
* **Gain:** Reduces raw ballot insertion statements by **200:1** ($200\times$ compression on the ballot table).

### Lever 3: Taking Tally Snapshots Off the Ingestion Hot Path
* **The Problem (Discovered during profiling):** Originally, every ballot batch flush also upserted a row into `precinct_tallies`. At batch size 50, this produced a 1:1 ratio between ballot inserts and tally upserts (~52% of all database statements), halving the effective compression ratio.
* **The Actor Solution:** The `:precinct` actor already holds the authoritative tally in memory and directly serves queries. Writing `precinct_tallies` on the ingestion path was redundant.
* **Implementation:** The batch-triggered flush passes `write-tally? = false` (`precinct.clj:167`). Only explicit flushes and long-interval checkpoints (`TICK_MAX_AGE_MS = 10000`) persist the summary row.
* **Gain:** Eliminated ~52% of total database operations under load.

### Lever 4: Staleness-Bounded Tick (`TICK_MIN_BATCH` vs `TICK_MAX_AGE_MS`)
* **The Problem:** When load was distributed across ~250 precincts, a naive 2-second periodic tick flushed prematurely, producing micro-batches of ~6.4 ballots instead of 200 (manufacturing 85,000 SQL statements for 547,000 ballots).
* **The Actor Solution:** The tick handler evaluates three conditions:
  ```clojure
  (if (or (zero? pending)
          (>= pending min-batch)     ; e.g. 200
          (>= age-ms max-age))       ; e.g. 10,000 ms
    (execute-flush-transition state false (:fencing ctx))
    [state {:status :deferred} []])
  ```
* **Gain:** Preserves full 200-row batch density during bursty traffic while guaranteeing that quiet precincts never linger unpersisted beyond 10 seconds.

### Lever 5: Hierarchical Delta Coalescing & Aggregation
* **The Problem:** If all 250 precincts updated the national election row on every vote, the database would suffer catastrophic row-level write lock contention on the single `election_tallies` record.
* **The Actor Solution:**
  1. **Coalesced Tells (`TELL_INTERVAL_MS = 200`):** Precinct actors accumulate candidate vote deltas in `:pending-deltas` and send an asynchronous `:precinct-delta` message to the parent `:election-tally` actor at most once every 200 ms.
  2. **Batch Aggregation (`FLUSH_EVERY_N = 50`):** The `:election-tally` actor merges deltas in memory and executes a single guarded `upsert!` only once every 50 updates:
  ```sql
  INSERT INTO election_tallies (election_id, tallies_json, precincts_reporting, total_votes, ownership_epoch, updated_at)
  VALUES ('national-2026', '{"alice": 45000, "bob": 42000}', 250, 87000, 14, NOW())
  ON CONFLICT (election_id) DO UPDATE SET ...
  WHERE election_tallies.ownership_epoch < EXCLUDED.ownership_epoch;
  ```
* **Gain:** 1 single national upsert covers tens of thousands of individual voter choices.

---

## 4. Visual Funnel: From 3,000 Votes/s to 35 SQL Stmts/s

```
[3,000 HTTP Votes / sec]  (Voter traffic across 250 precincts)
          │
          ▼
┌────────────────────────────────────────────────────────┐
│  API Tier (3x Replicas)                                │  -> Returns HTTP 202 Accepted (<2ms)
│  gRPC Publish to async-proxy                           │
└────────────────────────────────────────────────────────┘
          │
          ▼
┌────────────────────────────────────────────────────────┐
│  NATS JetStream (32 Partitions)                        │  -> Ingestion decoupled from DB speed
└────────────────────────────────────────────────────────┘
          │
          ▼
┌────────────────────────────────────────────────────────┐
│  Precinct Entity Actors (250 in-memory instances)      │
│  • O(1) Dedup (:voted-ids set)      -> 0 DB reads      │
│  • Buffer :pending-ballots          -> Accumulate 200  │
│  • Coalesce :pending-deltas         -> Flush every 200ms│
└────────────────────────────────────────────────────────┘
          │
          ├─────────────────────────────────────────┐
          │ (Every 200 ballots)                     │ (Every 200ms tell)
          ▼                                         ▼
┌──────────────────────────────────────┐  ┌──────────────────────────────────────┐
│ Ballot Batch Flush                   │  │ Parent :election-tally Actor         │
│ multi-row insert-batch!              │  │ Merges deltas into national totals   │
│ 3,000 votes/s ÷ 200 = ~15 stmts/s    │  │ Flushes every 50 updates = ~2 stmts/s│
└──────────────────────────────────────┘  └──────────────────────────────────────┘
          │                                         │
          └────────────────────┬────────────────────┘
                               ▼
┌────────────────────────────────────────────────────────────────────────────────┐
│  PostgreSQL 16 Database Tier                                                   │
│                                                                                │
│  Actual Executed SQL Statements: ~17 - 35 statements / sec                      │
│  Effective Write Compression Ratio: > 20x to 80x                               │
│  Postgres CPU: < 0.4 cores (Quiet, no throttling)                              │
└────────────────────────────────────────────────────────────────────────────────┘
```

---

## 5. Read Path Decoupling (Actor-First Reads)

A vital architectural requirement for write compression is **decoupling read freshness from write frequency**:
* In traditional systems, reads query the DB (`SELECT ... FROM election_tallies`), forcing the write path to persist immediately to maintain fresh numbers.
* In the Election System, `GET /elections/:id/tally` queries the actor state directly (`actor.peek_state` over internal transport).
* The in-memory actor serves the real-time aggregate in **0.1 ms** with zero DB load.
* Because reads do not depend on PostgreSQL, write batch buffers can comfortably dwell up to `TICK_MAX_AGE_MS` (10s) without degrading user-perceived freshness.

---

## 6. Monotonic Fencing & Split-Brain Protection

Under dynamic scaling, network partitions, or node failover, write batching must not introduce split-brain overwrites:
1. **Valkey Leases & Monotonic Fencing Epoch:**
   Each partition (`p00` through `p31`) has a lease key in Valkey with a monotonic fencing counter (`spec.leaseTransitions` / `INCR`).
2. **Delivery Context Injection:**
   The async proxy stamps the partition's validated `:fencing` epoch on every delivery envelope.
3. **Guarded SQL Upsert:**
   ```sql
   ON CONFLICT (election_id, precinct_id) DO UPDATE SET ...
   WHERE precinct_tallies.ownership_epoch < EXCLUDED.ownership_epoch;
   ```
4. **Invariant:** An evicted or lingering actor node whose lease expired will have its flush rejected at the database level if a successor node has already incremented the epoch. Stale re-flushes are safe no-ops.

---

## 7. Observability Verification in Grafana

The compression ratio is measured in real-time in Prometheus and Grafana:

* **Prometheus Metrics:**
  * `sql_batch_rows_total`: Count of logical rows persisted via batch operations.
  * `sql_statements_total`: Count of physical SQL statements issued across connections.
  * `sql_operation_duration_seconds`: Latency distribution per table and operation.
  * `container_cpu_cfs_throttled_periods_total`: cAdvisor CFS throttling ratio on the Postgres container.

* **The Compression Metric Formula (Grafana Panel):**
  $$\text{DB Write Compression Ratio} = \frac{\sum(\text{sql\_batch\_rows\_total})}{\max\left(\sum(\text{sql\_statements\_total}), 1\right)}$$

During the benchmark, this panel consistently registered **above 20.0**, climbing to **40–60+** during peak ballot ingestion bursts, demonstrating the stability and effectiveness of the actor-based persistence architecture.
