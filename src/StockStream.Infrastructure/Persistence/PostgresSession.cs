using System.Data;
using Npgsql;
using StockStream.Domain.Ports;

namespace StockStream.Infrastructure.Persistence;

/// <summary>
/// Manages a dedicated PostgreSQL TCP session per worker. Session-level advisory locks
/// are bound to the underlying PostgreSQL backend process, so Pooling is disabled to
/// guarantee a 1-to-1 relationship between the worker's session and its locks.
/// </summary>
public sealed class PostgresSession : IAsyncDisposable
{
    public NpgsqlConnection Connection { get; }

    private PostgresSession(NpgsqlConnection connection)
    {
        Connection = connection;
    }

    public static async Task<PostgresSession> ConnectWithRetryAsync(
        string databaseUrl,
        string applicationName,
        ISyncLogger? logger = null,
        int maxAttempts = 30,
        int retryDelayMs = 1000,
        CancellationToken ct = default)
    {
        var connectionString = NormalizeConnectionString(databaseUrl, applicationName);
        Exception? lastError = null;

        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            var conn = new NpgsqlConnection(connectionString);
            try
            {
                await conn.OpenAsync(ct);
                return new PostgresSession(conn);
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                lastError = ex;
                await conn.DisposeAsync();
                logger?.Warn("waiting for PostgreSQL connection", new { attempt, err = ex.Message });
                await Task.Delay(retryDelayMs, ct);
            }
        }

        throw new InvalidOperationException(
            $"Failed to connect to PostgreSQL after {maxAttempts} attempts.", lastError);
    }

    public async Task<T> ExecuteInTransactionAsync<T>(
        Func<NpgsqlConnection, NpgsqlTransaction, Task<T>> work,
        IsolationLevel isolationLevel = IsolationLevel.ReadCommitted,
        CancellationToken ct = default)
    {
        await using var tx = await Connection.BeginTransactionAsync(isolationLevel, ct);
        try
        {
            var result = await work(Connection, tx);
            await tx.CommitAsync(ct);
            return result;
        }
        catch
        {
            try
            {
                await tx.RollbackAsync(CancellationToken.None);
            }
            catch
            {
                // Connection may already be closed; PostgreSQL automatically rolls back on disconnect.
            }
            throw;
        }
    }

    public static string NormalizeConnectionString(string rawUrlOrConnString, string applicationName)
    {
        NpgsqlConnectionStringBuilder builder;

        if (rawUrlOrConnString.StartsWith("postgres://", StringComparison.OrdinalIgnoreCase) ||
            rawUrlOrConnString.StartsWith("postgresql://", StringComparison.OrdinalIgnoreCase))
        {
            var uri = new Uri(rawUrlOrConnString);
            var userInfo = uri.UserInfo.Split(':', 2);
            var username = Uri.UnescapeDataString(userInfo[0]);
            var password = userInfo.Length > 1 ? Uri.UnescapeDataString(userInfo[1]) : string.Empty;
            var database = uri.AbsolutePath.Trim('/');

            builder = new NpgsqlConnectionStringBuilder
            {
                Host = uri.Host,
                Port = uri.Port > 0 ? uri.Port : 5432,
                Username = username,
                Password = password,
                Database = database
            };
        }
        else
        {
            builder = new NpgsqlConnectionStringBuilder(rawUrlOrConnString);
        }

        // Dedicated session per worker so session-scoped advisory locks never leak across pool borrowers.
        builder.Pooling = false;
        builder.KeepAlive = 30;
        builder.ApplicationName = applicationName;

        return builder.ConnectionString;
    }

    public ValueTask DisposeAsync() => Connection.DisposeAsync();
}
