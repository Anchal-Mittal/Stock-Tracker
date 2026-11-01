using Npgsql;
using StockStream.Domain.Ports;

namespace StockStream.Infrastructure.Persistence;

/// <summary>
/// Applies purely additive, idempotent schema enhancements without modifying anything in schema.sql.
/// </summary>
public sealed class PostgresSchemaMigrator : ISchemaMigrator
{
    private const long MigrationAdvisoryLockKey = 0x5743_5354L; // "WCST"

    private const string MigrationSql = """
        -- Hard Guarantee #1: DB-enforced uniqueness on apply_log
        CREATE UNIQUE INDEX IF NOT EXISTS apply_log_message_id_uniq ON apply_log (message_id);
        CREATE UNIQUE INDEX IF NOT EXISTS apply_log_store_seq_uniq  ON apply_log (chain_id, store_id, seq);

        -- Speeds up receipt joins during candidate discovery and view loads
        CREATE INDEX IF NOT EXISTS queue_messages_receipt_idx ON queue_messages (receipt_id);

        -- Audit table for updates intentionally skipped (failed receipt, superseded before recovery snapshot, or blocked)
        -- Foreign key to queue_messages(id) ON DELETE CASCADE ensures `node seed.js` (TRUNCATE ... CASCADE) resets this table.
        CREATE TABLE IF NOT EXISTS skipped_updates (
          id              BIGSERIAL PRIMARY KEY,
          queue_row_id    BIGINT REFERENCES queue_messages (id) ON DELETE CASCADE,
          message_id      UUID,
          chain_id        UUID NOT NULL,
          store_id        UUID NOT NULL,
          seq             INTEGER NOT NULL,
          kind            TEXT,
          reason          TEXT NOT NULL,
          resolved_by_seq INTEGER,
          worker_id       TEXT NOT NULL,
          skipped_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        CREATE UNIQUE INDEX IF NOT EXISTS skipped_updates_store_seq_uniq ON skipped_updates (chain_id, store_id, seq);

        -- Disposable scheduling hints shared across workers (not a source of truth)
        CREATE TABLE IF NOT EXISTS sync_store_state (
          chain_id            UUID NOT NULL,
          store_id            UUID NOT NULL,
          status              TEXT NOT NULL CHECK (status IN ('waiting', 'gap', 'blocked', 'drained')),
          not_before          TIMESTAMPTZ,
          gap_seq             INTEGER,
          gap_since           TIMESTAMPTZ,
          blocked_seq         INTEGER,
          blocked_reason      TEXT,
          blocked_fingerprint TEXT,
          updated_by          TEXT NOT NULL,
          updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
          PRIMARY KEY (chain_id, store_id)
        );
        """;

    private readonly PostgresSession _session;

    public PostgresSchemaMigrator(PostgresSession session)
    {
        _session = session ?? throw new ArgumentNullException(nameof(session));
    }

    public async Task MigrateAsync(CancellationToken ct = default)
    {
        await using (var lockCmd = new NpgsqlCommand("SELECT pg_advisory_lock(@k)", _session.Connection))
        {
            lockCmd.Parameters.AddWithValue("k", MigrationAdvisoryLockKey);
            await lockCmd.ExecuteNonQueryAsync(ct);
        }

        try
        {
            await using var migrateCmd = new NpgsqlCommand(MigrationSql, _session.Connection);
            await migrateCmd.ExecuteNonQueryAsync(ct);
        }
        finally
        {
            await using var unlockCmd = new NpgsqlCommand("SELECT pg_advisory_unlock(@k)", _session.Connection);
            unlockCmd.Parameters.AddWithValue("k", MigrationAdvisoryLockKey);
            await unlockCmd.ExecuteNonQueryAsync(CancellationToken.None);
        }
    }

    public async Task ResetSchedulingHintsAsync(CancellationToken ct = default)
    {
        await using var cmd = new NpgsqlCommand("TRUNCATE sync_store_state", _session.Connection);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
