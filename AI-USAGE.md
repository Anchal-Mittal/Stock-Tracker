# AI-USAGE.md — Transparency on AI Assistance (.NET 8 / C# Edition)

## How AI Tools Were Used

We used an AI coding assistant in Antigravity as a pair-programming partner while designing and implementing this .NET 8 / C# solution. Below is an honest account of what was generated with AI assistance, what was architected and refined by hand, and concrete places where the AI's initial suggestions were wrong and we caught and fixed them.

### 1. What AI Generated (With Review)
- **Solution & Project Scaffolding**: `.csproj` files, `Directory.Build.props`, `ProcessorOptions` environment parsing, and `ConsoleSyncLogger`.
- **Boilerplate ADO.NET Parameter Binding**: Parameterized `NpgsqlCommand` boilerplate in `PostgresStoreSyncRepository` and `GroundTruthVerifier`.
- **xUnit Test Cases**: Translating the 12 domain planner edge cases into xUnit `[Fact]` tests in `StoreStreamPlannerTests.cs`.

### 2. What Was Designed & Reworked by Hand
- **DDD & SOLID Layering**: Separating the pure, zero-dependency domain model (`StockStream.Domain` — `StoreStreamPlanner`, `SyncDecision` discriminated union, Value Objects, and segregated port interfaces) from application orchestration (`StoreBacklogAdvancer`, `WorkerEngine`, `FleetSupervisor`) and `Npgsql` infrastructure (`PostgresSession`, `PostgresAdvisoryLeaseManager`, `PostgresStoreSyncRepository`).
- **Two-Layer Concurrency & Crash Safety**: Pairing non-blocking PostgreSQL session advisory locks (`pg_try_advisory_lock`) with row-level `FOR UPDATE` + `last_seq` Compare-And-Set (`WHERE s.last_seq = @expectedLastSeq`) and DB-level `UNIQUE` indexes on `apply_log`.
- **Skip-to-Snapshot Recovery Semantics**: Ensuring that when an update at `seq = K` has `ready_at IS NULL` (`--poison`), dependent deltas `K+1 .. M-1` are never applied on top of stale stock, while a subsequent `snapshot` at `seq = M > K` safely supersedes `K .. M-1` and records them in `skipped_updates`.

---

## Concrete Places Where the AI Got It Wrong (And How We Caught Them)

### Catch #1: `Npgsql` Connection Pooling Breaking Session-Level Advisory Locks
- **What the AI initially proposed**: Using `Npgsql`'s default connection string settings (`Pooling = true`) and opening/closing connections per operation or sharing an `NpgsqlDataSource` pool across calls.
- **Why it was broken**: PostgreSQL session-level advisory locks (`pg_try_advisory_lock`) are bound to the **underlying PostgreSQL backend TCP session**, not the application call site! With `Npgsql` connection pooling enabled, closing or returning a connection to the pool keeps the TCP session alive on the server (leaving the advisory lock held!), or a subsequent `ReleaseLeaseAsync` call can borrow a *different* physical connection from the pool and fail to unlock the store (`WARNING: you don't own a lock of type ExclusiveLock`).
- **How we caught and fixed it**: In `PostgresSession.NormalizeConnectionString`, we explicitly enforced `builder.Pooling = false` and `builder.KeepAlive = 30`, giving each worker process exactly one dedicated physical PostgreSQL session for its entire lifetime. When the worker holds a lease, `TryAcquireLeaseAsync`, `AdvanceStoreAsync`, and `ReleaseLeaseAsync` all execute on that single session, and if the worker crashes, the OS closes the socket and PostgreSQL releases the lock immediately.

### Catch #2: Spawning 4 Workers via `dotnet run` Causing MSBuild File-Lock Contention
- **What the AI initially proposed**: Having `FleetSupervisor` spawn 4 child processes using `dotnet run --project src/StockStream.Cli -- worker`.
- **Why it was broken**: Launching 4 concurrent `dotnet run` commands triggers 4 simultaneous MSBuild compilations of the same project directory (`obj/` and `bin/`), causing `MSB3026` file-lock collisions on Windows and adding several seconds of startup overhead on every worker respawn!
- **How we caught and fixed it**: In `Program.ResolveSelfProcessInvocation()`, the supervisor inspects `Environment.ProcessPath` and `AppContext.BaseDirectory` to invoke the **already-compiled** apphost executable (`StockStream.Cli.exe worker` or `dotnet exec StockStream.Cli.dll worker`) directly. Worker processes spawn in ~30ms with zero MSBuild contention.

### Catch #3: Stale Auxiliary Table State Surviving `node seed.js` Re-Runs
- **What the AI initially proposed**: Storing permanent store completion/block status in a custom table keyed only by `(chain_id, store_id)`.
- **Why it was broken**: Inspecting `seed.js` line 124 showed:
  ```js
  await client.query('TRUNCATE queue_messages, apply_log, store_stock, receipt_files RESTART IDENTITY CASCADE')
  ```
  Because `seed.js` uses a deterministic PRNG (`mulberry32(SEED)`), re-running `node seed.js` produces the **exact same `chain_id` and `store_id` UUIDs** while leaving standalone custom tables untouched!
- **How we fixed it**:
  1. Added `REFERENCES queue_messages(id) ON DELETE CASCADE` on `skipped_updates(queue_row_id)` so `seed.js`'s `TRUNCATE ... CASCADE` automatically wipes `skipped_updates`.
  2. Truncated `sync_store_state` in `FleetSupervisor` before spawning workers.
  3. Included `max(q.enqueued_at)` in `FingerprintSqlExpression` so any re-seed invalidates prior fingerprints even if a worker is run standalone.

### Catch #4: JSON Numeric Formatting Differences in `GroundTruthVerifier`
- **What the AI initially wrote**: Comparing `store_stock.stock::text` directly as a raw JSON string against `JsonSerializer.Serialize(expectedStock)`.
- **Why it was fragile**: PostgreSQL `jsonb::text` formats key order and whitespace canonically (`{"SKU-1002": 12.5, ...}`), whereas `System.Text.Json` formats without spaces after colons and preserves insertion order unless keys are sorted and scalar numbers are normalized (`G17` invariant culture).
- **How we caught and fixed it**: Built `CanonicalizeStockJson` and `CanonicalizeStockMap` in `GroundTruthVerifier` using `SortedDictionary<string, string>(StringComparer.Ordinal)` and `NormalizeScalar` so JSON comparison is order-independent and numerically exact.
