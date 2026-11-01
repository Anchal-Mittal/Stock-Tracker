using System.Data;
using System.Text;
using Npgsql;
using NpgsqlTypes;
using StockStream.Domain.Entities;
using StockStream.Domain.Ports;
using StockStream.Domain.ValueObjects;

namespace StockStream.Infrastructure.Persistence;

/// <summary>
/// Plain ADO.NET (<see cref="Npgsql"/>) implementation of <see cref="IStoreSyncRepository"/>.
/// Enforces safety via row-level FOR UPDATE + Compare-And-Set on store_stock.last_seq
/// inside a single atomic transaction with apply_log and skipped_updates.
/// </summary>
public sealed class PostgresStoreSyncRepository : IStoreSyncRepository
{
    /// <summary>
    /// DRY SQL expression computing a store's outstanding queue fingerprint.
    /// Any change to row count, ready receipt count, max arrival id, or enqueued_at invalidates a 'blocked' hint.
    /// </summary>
    private const string FingerprintSqlExpression =
        "count(*)::text || ':' || count(r.ready_at)::text || ':' || max(q.id)::text || ':' || COALESCE(extract(epoch FROM max(q.enqueued_at))::text, '')";

    private const string ViewRowsSql = """
        SELECT q.id,
               q.message_id,
               q.seq,
               q.kind,
               CASE WHEN r.ready_at IS NULL  THEN 'failed'
                    WHEN r.ready_at <= now() THEN 'ready'
                    ELSE 'pending' END        AS receipt_state,
               (extract(epoch FROM r.ready_at) * 1000)::float8 AS ready_at_ms,
               COALESCE(CASE
                 WHEN q.kind = 'snapshot' THEN jsonb_typeof(q.payload -> 'stock') = 'object'
                 ELSE jsonb_typeof(COALESCE(q.payload -> 'set', '{}'::jsonb)) = 'object'
                  AND jsonb_typeof(COALESCE(q.payload -> 'unset', '[]'::jsonb)) = 'array'
                  AND NOT EXISTS (
                        SELECT 1 FROM jsonb_array_elements(
                          CASE WHEN jsonb_typeof(q.payload -> 'unset') = 'array' THEN q.payload -> 'unset' ELSE '[]'::jsonb END
                        ) e WHERE jsonb_typeof(e) <> 'string')
               END, false)                    AS payload_valid
          FROM queue_messages q
          LEFT JOIN receipt_files r ON r.id = q.receipt_id
         WHERE q.chain_id = @chainId AND q.store_id = @storeId AND q.seq > @lastSeq
        """;

    private readonly PostgresSession _session;

    public PostgresStoreSyncRepository(PostgresSession session)
    {
        _session = session ?? throw new ArgumentNullException(nameof(session));
    }

    public Task<StoreStreamSnapshot> LoadStoreViewAsync(
        StoreIdentity store,
        double gapTimeoutMs,
        CancellationToken ct = default)
    {
        return _session.ExecuteInTransactionAsync(
            async (conn, tx) =>
            {
                // Enforce READ ONLY on the repeatable-read snapshot
                await using (var roCmd = new NpgsqlCommand("SET TRANSACTION READ ONLY", conn, tx))
                {
                    await roCmd.ExecuteNonQueryAsync(ct);
                }

                int lastSeq;
                double nowMs;
                int? gapSeq = null;
                double? gapSinceMs = null;

                const string headSql = """
                    SELECT COALESCE((SELECT last_seq FROM store_stock WHERE chain_id = @chainId AND store_id = @storeId), 0) AS last_seq,
                           (extract(epoch FROM now()) * 1000)::float8 AS now_ms,
                           st.gap_seq,
                           (extract(epoch FROM st.gap_since) * 1000)::float8 AS gap_since_ms
                      FROM (SELECT 1) one
                      LEFT JOIN sync_store_state st ON st.chain_id = @chainId AND st.store_id = @storeId
                    """;

                await using (var headCmd = new NpgsqlCommand(headSql, conn, tx))
                {
                    headCmd.Parameters.AddWithValue("chainId", store.ChainId);
                    headCmd.Parameters.AddWithValue("storeId", store.StoreId);

                    await using var reader = await headCmd.ExecuteReaderAsync(ct);
                    await reader.ReadAsync(ct);
                    lastSeq = reader.GetInt32(0);
                    nowMs = reader.GetDouble(1);
                    if (!reader.IsDBNull(2)) gapSeq = reader.GetInt32(2);
                    if (!reader.IsDBNull(3)) gapSinceMs = reader.GetDouble(3);
                }

                var rows = new List<QueuedUpdate>();
                await using (var rowsCmd = new NpgsqlCommand(ViewRowsSql, conn, tx))
                {
                    rowsCmd.Parameters.AddWithValue("chainId", store.ChainId);
                    rowsCmd.Parameters.AddWithValue("storeId", store.StoreId);
                    rowsCmd.Parameters.AddWithValue("lastSeq", lastSeq);

                    await using var reader = await rowsCmd.ExecuteReaderAsync(ct);
                    while (await reader.ReadAsync(ct))
                    {
                        var id = reader.GetInt64(0);
                        var messageId = reader.GetGuid(1);
                        var seq = reader.GetInt32(2);
                        var kind = DomainEnumMappings.ParseUpdateKind(reader.GetString(3));
                        var receiptState = DomainEnumMappings.ParseReceiptAvailability(reader.GetString(4));
                        double? readyAtMs = reader.IsDBNull(5) ? null : reader.GetDouble(5);
                        var payloadValid = reader.GetBoolean(6);

                        rows.Add(new QueuedUpdate(id, messageId, seq, kind, receiptState, readyAtMs, payloadValid));
                    }
                }

                SequenceGapTimer? gapTimer = gapSeq.HasValue && gapSinceMs.HasValue
                    ? new SequenceGapTimer(gapSeq.Value, gapSinceMs.Value)
                    : null;

                return new StoreStreamSnapshot(store, lastSeq, rows, nowMs, gapTimer, gapTimeoutMs);
            },
            IsolationLevel.RepeatableRead,
            ct);
    }

    public async Task<ApplyOutcome> ApplyUpdateAsync(
        StoreIdentity store,
        QueuedUpdate row,
        int expectedLastSeq,
        IReadOnlyList<SkippedUpdate> skipped,
        string workerId,
        Action? midTransactionHook = null,
        CancellationToken ct = default)
    {
        try
        {
            return await _session.ExecuteInTransactionAsync(
                async (conn, tx) =>
                {
                    // 1. Lock the store_stock row FOR UPDATE and verify last_seq matches expectedLastSeq (CAS)
                    var currentLastSeq = 0;
                    await using (var lockCmd = new NpgsqlCommand(
                        "SELECT last_seq FROM store_stock WHERE chain_id = @c AND store_id = @s FOR UPDATE",
                        conn, tx))
                    {
                        lockCmd.Parameters.AddWithValue("c", store.ChainId);
                        lockCmd.Parameters.AddWithValue("s", store.StoreId);
                        var scalar = await lockCmd.ExecuteScalarAsync(ct);
                        if (scalar is int existingSeq) currentLastSeq = existingSeq;
                    }

                    if (currentLastSeq != expectedLastSeq)
                    {
                        throw new TransactionalAbortException(ApplyAbortReason.CasConflict);
                    }

                    // 2. Re-verify receipt readiness against PostgreSQL's now() inside the write transaction
                    await using (var receiptCmd = new NpgsqlCommand(
                        """
                        SELECT (r.ready_at IS NOT NULL AND r.ready_at <= now()) AS ready
                          FROM queue_messages q
                          JOIN receipt_files r ON r.id = q.receipt_id
                         WHERE q.id = @rowId
                        """,
                        conn, tx))
                    {
                        receiptCmd.Parameters.AddWithValue("rowId", row.QueueRowId);
                        var readyObj = await receiptCmd.ExecuteScalarAsync(ct);
                        if (readyObj is not true)
                        {
                            throw new TransactionalAbortException(ApplyAbortReason.ReceiptNotReady);
                        }
                    }

                    // 3. Apply snapshot replacement or delta merge directly in PostgreSQL JSONB
                    int? newVersion;
                    if (row.Kind == UpdateKind.Snapshot)
                    {
                        await using var snapCmd = new NpgsqlCommand(
                            """
                            INSERT INTO store_stock AS s (chain_id, store_id, stock, version, last_seq, updated_at)
                            SELECT q.chain_id, q.store_id, q.payload -> 'stock', 1, q.seq, now()
                              FROM queue_messages q WHERE q.id = @rowId
                            ON CONFLICT (chain_id, store_id) DO UPDATE
                               SET stock = EXCLUDED.stock,
                                   version = s.version + 1,
                                   last_seq = EXCLUDED.last_seq,
                                   updated_at = now()
                             WHERE s.last_seq = @expectedLastSeq
                            RETURNING s.version
                            """,
                            conn, tx);
                        snapCmd.Parameters.AddWithValue("rowId", row.QueueRowId);
                        snapCmd.Parameters.AddWithValue("expectedLastSeq", expectedLastSeq);
                        newVersion = (int?)await snapCmd.ExecuteScalarAsync(ct);
                    }
                    else
                    {
                        if (row.Seq != expectedLastSeq + 1)
                        {
                            throw new InvalidOperationException(
                                $"Refusing to apply delta seq={row.Seq} on last_seq={expectedLastSeq}.");
                        }

                        await using var deltaCmd = new NpgsqlCommand(
                            """
                            UPDATE store_stock AS s
                               SET stock = (s.stock || COALESCE(q.payload -> 'set', '{}'::jsonb))
                                           - COALESCE(ARRAY(SELECT jsonb_array_elements_text(q.payload -> 'unset')), '{}'::text[]),
                                   version = s.version + 1,
                                   last_seq = q.seq,
                                   updated_at = now()
                              FROM queue_messages q
                             WHERE q.id = @rowId
                               AND s.chain_id = q.chain_id AND s.store_id = q.store_id
                               AND s.last_seq = @expectedLastSeq
                            RETURNING s.version
                            """,
                            conn, tx);
                        deltaCmd.Parameters.AddWithValue("rowId", row.QueueRowId);
                        deltaCmd.Parameters.AddWithValue("expectedLastSeq", expectedLastSeq);
                        newVersion = (int?)await deltaCmd.ExecuteScalarAsync(ct);
                    }

                    if (newVersion is null)
                    {
                        throw new TransactionalAbortException(ApplyAbortReason.CasConflict);
                    }

                    // Optional fault-injection hook for chaos testing (fires after store_stock write, before apply_log & COMMIT)
                    midTransactionHook?.Invoke();

                    // 4. Append audit record to apply_log
                    await using (var logCmd = new NpgsqlCommand(
                        """
                        INSERT INTO apply_log (message_id, chain_id, store_id, seq, kind, worker_id)
                        VALUES (@msgId, @chainId, @storeId, @seq, @kind, @workerId)
                        """,
                        conn, tx))
                    {
                        logCmd.Parameters.AddWithValue("msgId", row.MessageId);
                        logCmd.Parameters.AddWithValue("chainId", store.ChainId);
                        logCmd.Parameters.AddWithValue("storeId", store.StoreId);
                        logCmd.Parameters.AddWithValue("seq", row.Seq);
                        logCmd.Parameters.AddWithValue("kind", row.Kind.ToDatabaseValue());
                        logCmd.Parameters.AddWithValue("workerId", workerId);
                        await logCmd.ExecuteNonQueryAsync(ct);
                    }

                    // 5. Record any skipped updates superseded by this recovery snapshot
                    if (skipped.Count > 0)
                    {
                        await RecordSkippedInternalAsync(conn, tx, store, skipped, row.Seq, workerId, ct);
                    }

                    await using (var cleanCmd = new NpgsqlCommand(
                        "DELETE FROM skipped_updates WHERE chain_id = @c AND store_id = @s AND seq = @seq",
                        conn, tx))
                    {
                        cleanCmd.Parameters.AddWithValue("c", store.ChainId);
                        cleanCmd.Parameters.AddWithValue("s", store.StoreId);
                        cleanCmd.Parameters.AddWithValue("seq", row.Seq);
                        await cleanCmd.ExecuteNonQueryAsync(ct);
                    }

                    return ApplyOutcome.Success(newVersion.Value);
                },
                IsolationLevel.ReadCommitted,
                ct);
        }
        catch (TransactionalAbortException abortEx)
        {
            return ApplyOutcome.Aborted(abortEx.Reason);
        }
    }

    public Task RecordSkippedUpdatesAsync(
        StoreIdentity store,
        IReadOnlyList<SkippedUpdate> skipped,
        int? resolvedBySeq,
        string workerId,
        CancellationToken ct = default)
    {
        if (skipped.Count == 0) return Task.CompletedTask;
        return RecordSkippedInternalAsync(_session.Connection, null, store, skipped, resolvedBySeq, workerId, ct);
    }

    private static async Task RecordSkippedInternalAsync(
        NpgsqlConnection conn,
        NpgsqlTransaction? tx,
        StoreIdentity store,
        IReadOnlyList<SkippedUpdate> skipped,
        int? resolvedBySeq,
        string workerId,
        CancellationToken ct)
    {
        var sb = new StringBuilder(
            "INSERT INTO skipped_updates (queue_row_id, message_id, chain_id, store_id, seq, kind, reason, resolved_by_seq, worker_id) VALUES ");

        await using var cmd = new NpgsqlCommand { Connection = conn, Transaction = tx };

        for (var i = 0; i < skipped.Count; i++)
        {
            if (i > 0) sb.Append(',');
            var s = skipped[i];
            sb.Append($"(@qid{i}, @mid{i}, @cid{i}, @sid{i}, @seq{i}, @kind{i}, @reason{i}, @res{i}, @wid{i})");

            cmd.Parameters.Add(new NpgsqlParameter($"qid{i}", NpgsqlDbType.Bigint)
            {
                Value = s.Row is not null ? s.Row.QueueRowId : DBNull.Value
            });
            cmd.Parameters.Add(new NpgsqlParameter($"mid{i}", NpgsqlDbType.Uuid)
            {
                Value = s.Row is not null ? s.Row.MessageId : DBNull.Value
            });
            cmd.Parameters.AddWithValue($"cid{i}", store.ChainId);
            cmd.Parameters.AddWithValue($"sid{i}", store.StoreId);
            cmd.Parameters.AddWithValue($"seq{i}", s.Seq);
            cmd.Parameters.Add(new NpgsqlParameter($"kind{i}", NpgsqlDbType.Text)
            {
                Value = s.Row is not null ? s.Row.Kind.ToDatabaseValue() : DBNull.Value
            });
            cmd.Parameters.AddWithValue($"reason{i}", s.Reason.ToDatabaseValue());
            cmd.Parameters.Add(new NpgsqlParameter($"res{i}", NpgsqlDbType.Integer)
            {
                Value = resolvedBySeq.HasValue ? resolvedBySeq.Value : DBNull.Value
            });
            cmd.Parameters.AddWithValue($"wid{i}", workerId);
        }

        sb.Append("""
             ON CONFLICT (chain_id, store_id, seq) DO UPDATE
                SET queue_row_id = EXCLUDED.queue_row_id,
                    message_id = EXCLUDED.message_id,
                    kind = EXCLUDED.kind,
                    reason = EXCLUDED.reason,
                    resolved_by_seq = EXCLUDED.resolved_by_seq,
                    worker_id = EXCLUDED.worker_id,
                    skipped_at = now()
            """);

        cmd.CommandText = sb.ToString();
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<string> ComputeStoreFingerprintAsync(StoreIdentity store, CancellationToken ct = default)
    {
        var sql = $"""
            SELECT {FingerprintSqlExpression} AS fp
              FROM queue_messages q
              JOIN receipt_files r ON r.id = q.receipt_id
             WHERE q.chain_id = @chainId AND q.store_id = @storeId
               AND q.seq > COALESCE((SELECT last_seq FROM store_stock s WHERE s.chain_id = @chainId AND s.store_id = @storeId), 0)
            """;

        await using var cmd = new NpgsqlCommand(sql, _session.Connection);
        cmd.Parameters.AddWithValue("chainId", store.ChainId);
        cmd.Parameters.AddWithValue("storeId", store.StoreId);
        var res = await cmd.ExecuteScalarAsync(ct);
        return res as string ?? "0:0::";
    }

    public async Task UpdateSchedulingHintAsync(
        StoreIdentity store,
        string workerId,
        StoreSchedulingHint hint,
        CancellationToken ct = default)
    {
        const string sql = """
            INSERT INTO sync_store_state AS st
                    (chain_id, store_id, status, not_before, gap_seq, gap_since, blocked_seq, blocked_reason, blocked_fingerprint, updated_by, updated_at)
            VALUES (@chainId, @storeId, @status, to_timestamp(@notBeforeMs / 1000.0), @gapSeq,
                    CASE WHEN @gapSeq IS NULL THEN NULL ELSE now() END,
                    @blockedSeq, @blockedReason, @blockedFp, @workerId, now())
            ON CONFLICT (chain_id, store_id) DO UPDATE
               SET status = EXCLUDED.status,
                   not_before = EXCLUDED.not_before,
                   gap_seq = EXCLUDED.gap_seq,
                   gap_since = CASE WHEN EXCLUDED.gap_seq IS NULL THEN NULL
                                    WHEN st.gap_seq IS NOT DISTINCT FROM EXCLUDED.gap_seq AND st.gap_since IS NOT NULL THEN st.gap_since
                                    ELSE now() END,
                   blocked_seq = EXCLUDED.blocked_seq,
                   blocked_reason = EXCLUDED.blocked_reason,
                   blocked_fingerprint = EXCLUDED.blocked_fingerprint,
                   updated_by = EXCLUDED.updated_by,
                   updated_at = now()
            """;

        await using var cmd = new NpgsqlCommand(sql, _session.Connection);
        cmd.Parameters.AddWithValue("chainId", store.ChainId);
        cmd.Parameters.AddWithValue("storeId", store.StoreId);
        cmd.Parameters.AddWithValue("status", hint.Status.ToDatabaseValue());
        cmd.Parameters.Add(new NpgsqlParameter("notBeforeMs", NpgsqlDbType.Double)
        {
            Value = hint.NotBeforeEpochMs.HasValue ? hint.NotBeforeEpochMs.Value : DBNull.Value
        });
        cmd.Parameters.Add(new NpgsqlParameter("gapSeq", NpgsqlDbType.Integer)
        {
            Value = hint.GapSeq.HasValue ? hint.GapSeq.Value : DBNull.Value
        });
        cmd.Parameters.Add(new NpgsqlParameter("blockedSeq", NpgsqlDbType.Integer)
        {
            Value = hint.BlockedSeq.HasValue ? hint.BlockedSeq.Value : DBNull.Value
        });
        cmd.Parameters.Add(new NpgsqlParameter("blockedReason", NpgsqlDbType.Text)
        {
            Value = hint.BlockedReason is not null ? hint.BlockedReason : DBNull.Value
        });
        cmd.Parameters.Add(new NpgsqlParameter("blockedFp", NpgsqlDbType.Text)
        {
            Value = hint.BlockedFingerprint is not null ? hint.BlockedFingerprint : DBNull.Value
        });
        cmd.Parameters.AddWithValue("workerId", workerId);

        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<CandidateBatch> FindCandidatesAsync(CancellationToken ct = default)
    {
        var sql = $"""
            WITH outstanding AS (
              SELECT q.chain_id, q.store_id, {FingerprintSqlExpression} AS fp
                FROM queue_messages q
                JOIN receipt_files r ON r.id = q.receipt_id
                LEFT JOIN store_stock s ON s.chain_id = q.chain_id AND s.store_id = q.store_id
               WHERE q.seq > COALESCE(s.last_seq, 0)
               GROUP BY q.chain_id, q.store_id
            )
            SELECT o.chain_id, o.store_id, st.status,
                   (extract(epoch FROM st.not_before) * 1000)::float8 AS not_before_ms,
                   (st.not_before IS NULL OR st.not_before <= now()) AS due,
                   (extract(epoch FROM now()) * 1000)::float8 AS now_ms
              FROM outstanding o
              LEFT JOIN sync_store_state st ON st.chain_id = o.chain_id AND st.store_id = o.store_id
             WHERE NOT (st.status IS NOT DISTINCT FROM 'blocked' AND st.blocked_fingerprint IS NOT DISTINCT FROM o.fp)
            """;

        var candidates = new List<StoreCandidate>();
        double nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        await using var cmd = new NpgsqlCommand(sql, _session.Connection);
        await using var reader = await cmd.ExecuteReaderAsync(ct);

        while (await reader.ReadAsync(ct))
        {
            var chainId = reader.GetGuid(0);
            var storeId = reader.GetGuid(1);
            var status = reader.IsDBNull(2) ? null : reader.GetString(2);
            double? notBeforeMs = reader.IsDBNull(3) ? null : reader.GetDouble(3);
            var isDue = reader.GetBoolean(4);
            nowMs = reader.GetDouble(5);

            candidates.Add(new StoreCandidate(new StoreIdentity(chainId, storeId), isDue, notBeforeMs, status));
        }

        return new CandidateBatch(candidates, nowMs);
    }

    public async Task<string> ComputeQueueSignatureAsync(CancellationToken ct = default)
    {
        const string sql = """
            SELECT count(*)::text || ':' || COALESCE(max(id), 0)::text || ':' ||
                   (SELECT count(*) FROM receipt_files WHERE ready_at IS NOT NULL)::text AS sig
              FROM queue_messages
            """;

        await using var cmd = new NpgsqlCommand(sql, _session.Connection);
        var res = await cmd.ExecuteScalarAsync(ct);
        return res as string ?? "0:0:0";
    }

    public async Task<FleetSyncSummary> GetFleetSummaryAsync(CancellationToken ct = default)
    {
        const string sql = """
            SELECT
              (SELECT count(*)::int FROM store_stock) AS stores_updated,
              (SELECT count(*)::int FROM apply_log) AS total_applied,
              (SELECT count(DISTINCT worker_id)::int FROM apply_log) AS distinct_workers,
              (SELECT count(*)::int FROM skipped_updates) AS total_skipped,
              (SELECT count(*)::int FROM sync_store_state WHERE status = 'blocked') AS blocked_stores
            """;

        await using var cmd = new NpgsqlCommand(sql, _session.Connection);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        await reader.ReadAsync(ct);

        return new FleetSyncSummary(
            StoresUpdated: reader.GetInt32(0),
            UpdatesApplied: reader.GetInt32(1),
            DistinctWorkersInLog: reader.GetInt32(2),
            UpdatesSkipped: reader.GetInt32(3),
            BlockedStores: reader.GetInt32(4));
    }

    private sealed class TransactionalAbortException(ApplyAbortReason reason) : Exception
    {
        public ApplyAbortReason Reason { get; } = reason;
    }
}
