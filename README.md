# StockStream — Backlog Sync Processor (.NET 8 / C# Edition)

A crash-resilient, multi-process queue processor built with **.NET 8 (C# 12)** following **Domain-Driven Design (DDD)**, **SOLID**, and **DRY** principles. It drains `queue_messages` across **4 OS worker processes**, replays store snapshots and deltas in strict per-store sequence order into `store_stock`, and writes an exact, deduplicated audit trail to `apply_log` using plain ADO.NET (`Npgsql` — zero ORMs, zero job/queue frameworks).

---

## Quick Start

```bash
# 1. Start PostgreSQL 16 (port 5439)
docker compose up -d

# 2. Install Node dependency for the provided seed script & seed the queue (~2,000 rows)
npm install
node seed.js

# 3. Build & run the processor (one command brings up the supervisor + all 4 OS worker processes)
dotnet run --project src/StockStream.Cli
```

---

## Verification, Unit Tests & Chaos Suite

```bash
# Independently replay the queue stream in C# and verify 7 invariants across
# store_stock, apply_log, receipt_files, and skipped_updates:
dotnet run --project src/StockStream.Cli -- verify

# Run the xUnit Domain & Application test suite (12 unit test cases):
dotnet test

# Run the automated hostile chaos suite (poisoned receipts, mid-transaction
# worker kills, full fleet kill & restart mid-run, re-seeding with different
# SEED, and skip-to-snapshot recovery):
dotnet run --project src/StockStream.Cli -- chaos
```

### Poison Mode

```bash
node seed.js --poison
dotnet run --project src/StockStream.Cli
dotnet run --project src/StockStream.Cli -- verify
```

---

## Solution Architecture (DDD + SOLID + DRY)

```text
dotnet-stockstream/
├── StockStream.sln
├── Directory.Build.props                    # DRY build properties (net8.0, Nullable, TreatWarningsAsErrors)
├── docker-compose.yml                       # Provided PostgreSQL 16 service (unchanged)
├── schema.sql                               # Provided base schema (unchanged)
├── seed.js                                  # Provided deterministic queue seeder (unchanged)
├── src/
│   ├── StockStream.Domain/                  # Pure Domain Layer (Zero external dependencies)
│   │   ├── ValueObjects/
│   │   │   ├── StoreIdentity.cs             # Value Object: (ChainId, StoreId)
│   │   │   └── Enumerations.cs              # UpdateKind, ReceiptAvailability, SkipReason, StoreSyncStatus
│   │   ├── Entities/
│   │   │   └── StoreStreamEntities.cs       # QueuedUpdate, SkippedUpdate, SequenceGapTimer, StoreStreamSnapshot
│   │   ├── Decisions/
│   │   │   └── SyncDecision.cs              # Closed discriminated union (Apply, Wait, Gap, Block, Drained)
│   │   ├── Services/
│   │   │   ├── IStoreStreamPlanner.cs       # Pure domain planner contract
│   │   │   └── StoreStreamPlanner.cs        # Ordering, dedup, receipt barrier, skip-to-snapshot recovery
│   │   └── Ports/
│   │       └── Ports.cs                     # Segregated ports: IStoreLeaseManager, ISchemaMigrator, IStoreSyncRepository, ISyncLogger
│   ├── StockStream.Application/             # Application / Use-Case Layer (Depends only on Domain)
│   │   ├── Configuration/
│   │   │   └── ProcessorOptions.cs          # Immutable runtime configuration
│   │   ├── Logging/
│   │   │   └── ConsoleSyncLogger.cs         # Structured per-PID JSON logger
│   │   └── Services/
│   │       ├── StoreBacklogAdvancer.cs      # Use case: advances a single leased store stream
│   │       ├── WorkerEngine.cs              # Use case: worker poll/claim/advance/drain loop
│   │       └── FleetSupervisor.cs           # Use case: spawns & monitors 4 OS worker processes
│   ├── StockStream.Infrastructure/          # Infrastructure Layer (Plain Npgsql ADO.NET)
│   │   ├── Locking/
│   │   │   └── PostgresAdvisoryLeaseManager.cs  # pg_try_advisory_lock / pg_advisory_unlock
│   │   └── Persistence/
│   │       ├── PostgresSession.cs           # Dedicated unpooled TCP session (KeepAlive=30s) & transaction wrapper
│   │       ├── PostgresSchemaMigrator.cs    # Idempotent additive DDL migrations
│   │       └── PostgresStoreSyncRepository.cs   # REPEATABLE READ view loader + FOR UPDATE / last_seq CAS writes
│   └── StockStream.Cli/                     # Entrypoint, Ground-Truth Verifier & Chaos Runner
│       ├── Program.cs                       # Commands: supervisor (default) | worker | verify | chaos
│       └── Verification/
│           ├── GroundTruthVerifier.cs       # Independent C# stream replay & 7-invariant checker
│           └── ChaosSuiteRunner.cs          # Automated hostile-conditions runner
└── tests/
    └── StockStream.Tests/                   # xUnit tests for Domain & Application layers
        └── StoreStreamPlannerTests.cs
```

---

## Configuration (Environment Variables)

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `postgres://postgres:postgres@localhost:5439/stockstream` | PostgreSQL connection URI or Npgsql connection string |
| `WORKER_COUNT` | `4` | Number of OS worker processes maintained by `FleetSupervisor` |
| `MAX_IDLE_SLEEP_MS` | `400` | Maximum polling sleep when waiting on future receipts (`≤ 500 ms` per spec) |
| `MIN_IDLE_SLEEP_MS` | `50` | Minimum backoff between polls |
| `DRAIN_QUIET_MS` | `800` | Quiet window with zero pending work before workers exit `0` |
| `GAP_TIMEOUT_MS` | `60000` | How long a missing `seq` is waited on before failing over to a later snapshot |
| `MAX_RESTARTS_PER_SLOT` | `25` | Maximum times the supervisor respawns a crashed worker slot |
| `CHAOS_CRASH_PROB` | `0` | Fault injection (`0..1`): probability of self-killing mid-transaction before `COMMIT` |
| `LOG_LEVEL` | `info` | `debug` \| `info` \| `warn` \| `error` |
