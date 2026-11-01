using Npgsql;
using StockStream.Domain.Ports;
using StockStream.Domain.ValueObjects;
using StockStream.Infrastructure.Persistence;

namespace StockStream.Infrastructure.Locking;

/// <summary>
/// Acquires and releases non-blocking PostgreSQL session-level advisory locks per store.
/// Automatically released by PostgreSQL the instant a worker process crashes or disconnects.
/// </summary>
public sealed class PostgresAdvisoryLeaseManager : IStoreLeaseManager
{
    private const string TryLockSql = "SELECT pg_try_advisory_lock(hashtextextended(@key, 0))";
    private const string UnlockSql = "SELECT pg_advisory_unlock(hashtextextended(@key, 0))";

    private readonly PostgresSession _session;

    public PostgresAdvisoryLeaseManager(PostgresSession session)
    {
        _session = session ?? throw new ArgumentNullException(nameof(session));
    }

    public async Task<bool> TryAcquireLeaseAsync(StoreIdentity store, CancellationToken ct = default)
    {
        await using var cmd = new NpgsqlCommand(TryLockSql, _session.Connection);
        cmd.Parameters.AddWithValue("key", store.ToCompositeKey());
        var result = await cmd.ExecuteScalarAsync(ct);
        return result is true;
    }

    public async Task ReleaseLeaseAsync(StoreIdentity store, CancellationToken ct = default)
    {
        await using var cmd = new NpgsqlCommand(UnlockSql, _session.Connection);
        cmd.Parameters.AddWithValue("key", store.ToCompositeKey());
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
