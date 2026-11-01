using System.Diagnostics;
using Npgsql;
using StockStream.Application.Configuration;
using StockStream.Infrastructure.Persistence;

namespace StockStream.Cli.Verification;

/// <summary>
/// Automated hostile evaluation test runner exercising:
///   1. Poisoned receipts (--poison) + mid-transaction worker kills (CHAOS_CRASH_PROB) + mid-run full supervisor kill &amp; restart
///   2. Re-seeding with a different SEED (SEED=1337) + deterministic skip-to-snapshot recovery
/// </summary>
public sealed class ChaosSuiteRunner
{
    private readonly ProcessorOptions _options;
    private readonly string _repoRoot;
    private readonly string _workerExePath;
    private readonly string[] _workerBaseArgs;

    public ChaosSuiteRunner(
        ProcessorOptions options,
        string repoRoot,
        string workerExePath,
        string[] workerBaseArgs)
    {
        _options = options;
        _repoRoot = repoRoot;
        _workerExePath = workerExePath;
        _workerBaseArgs = workerBaseArgs;
    }

    public async Task RunAllScenariosAsync(CancellationToken ct = default)
    {
        var seedScript = Path.Combine(_repoRoot, "seed.js");
        if (!File.Exists(seedScript))
        {
            throw new FileNotFoundException($"Could not locate seed.js at '{seedScript}'.");
        }

        Console.WriteLine("\n=== Scenario 1: SEED=42 --poison + mid-txn worker kills + mid-run full fleet restart ===");
        await RunExternalProcessAsync("node", [seedScript, "--poison"], new Dictionary<string, string>
        {
            ["SEED"] = "42",
            ["DATABASE_URL"] = _options.DatabaseUrl
        }, ct);

        // Start supervisor with 1.5% mid-transaction crash probability and kill the entire process tree after 2.5s
        await RunSupervisorThenKillAfterDelayAsync(TimeSpan.FromMilliseconds(2500), new Dictionary<string, string>
        {
            ["CHAOS_CRASH_PROB"] = "0.015",
            ["LOG_LEVEL"] = "warn",
            ["DATABASE_URL"] = _options.DatabaseUrl
        }, ct);

        Console.WriteLine("[chaos] restarting .NET supervisor to finish draining backlog...");
        await RunExternalProcessAsync(_workerExePath, [.. _workerBaseArgs, "supervisor"], new Dictionary<string, string>
        {
            ["CHAOS_CRASH_PROB"] = "0.01",
            ["DRAIN_QUIET_MS"] = "500",
            ["LOG_LEVEL"] = "info",
            ["DATABASE_URL"] = _options.DatabaseUrl
        }, ct);

        await AssertVerifiedAsync("Scenario 1 (poison + mid-txn kill + full restart)", ct);

        Console.WriteLine("\n=== Scenario 2: Re-seed SEED=1337 + skip-to-snapshot recovery ===");
        await RunExternalProcessAsync("node", [seedScript], new Dictionary<string, string>
        {
            ["SEED"] = "1337",
            ["DATABASE_URL"] = _options.DatabaseUrl
        }, ct);

        await InjectSkipToSnapshotPoisonAsync(ct);

        await RunExternalProcessAsync(_workerExePath, [.. _workerBaseArgs, "supervisor"], new Dictionary<string, string>
        {
            ["DRAIN_QUIET_MS"] = "400",
            ["LOG_LEVEL"] = "warn",
            ["DATABASE_URL"] = _options.DatabaseUrl
        }, ct);

        var report2 = await AssertVerifiedAsync("Scenario 2 (re-seed SEED=1337 + skip-to-snapshot recovery)", ct);
        if (report2.TotalSkipped == 0)
        {
            throw new InvalidOperationException("Expected skipped updates from skip-to-snapshot recovery, but got 0.");
        }

        Console.WriteLine("\nALL .NET CHAOS SCENARIOS PASSED.");
    }

    private async Task InjectSkipToSnapshotPoisonAsync(CancellationToken ct)
    {
        await using var session = await PostgresSession.ConnectWithRetryAsync(_options.DatabaseUrl, "chaos-injector", ct: ct);
        var conn = session.Connection;

        await using (var ffCmd = new NpgsqlCommand(
            "UPDATE receipt_files SET ready_at = now() - interval '1 second' WHERE ready_at > now()", conn))
        {
            await ffCmd.ExecuteNonQueryAsync(ct);
        }

        Guid? chainId = null;
        Guid? storeId = null;
        int? snapSeq = null;

        await using (var findCmd = new NpgsqlCommand(
            "SELECT chain_id, store_id, seq FROM queue_messages WHERE kind = 'snapshot' AND seq > 3 LIMIT 1", conn))
        await using (var r = await findCmd.ExecuteReaderAsync(ct))
        {
            if (await r.ReadAsync(ct))
            {
                chainId = r.GetGuid(0);
                storeId = r.GetGuid(1);
                snapSeq = r.GetInt32(2);
            }
        }

        if (chainId.HasValue && storeId.HasValue && snapSeq.HasValue)
        {
            await using var poisonCmd = new NpgsqlCommand(
                """
                UPDATE receipt_files
                   SET ready_at = NULL
                 WHERE id IN (
                   SELECT receipt_id FROM queue_messages
                    WHERE chain_id = @c AND store_id = @s AND seq = 2
                 )
                """, conn);
            poisonCmd.Parameters.AddWithValue("c", chainId.Value);
            poisonCmd.Parameters.AddWithValue("s", storeId.Value);
            await poisonCmd.ExecuteNonQueryAsync(ct);

            Console.WriteLine($"[chaos] poisoned seq=2 of store {storeId.Value} which has a recovery snapshot at seq={snapSeq.Value}");
        }
    }

    private async Task<VerificationReport> AssertVerifiedAsync(string label, CancellationToken ct)
    {
        var verifier = new GroundTruthVerifier(_options.DatabaseUrl);
        var report = await verifier.VerifyAsync(ct);
        if (!report.Ok)
        {
            Console.Error.WriteLine($"[chaos] FAIL in \"{label}\":\n  " + string.Join("\n  ", report.Violations));
            Environment.Exit(1);
        }

        Console.WriteLine(
            $"[chaos] PASS \"{label}\" — stores={report.StoresChecked} applied={report.TotalApplied} skipped={report.TotalSkipped} workers={report.DistinctWorkers.Count}");
        return report;
    }

    private async Task RunSupervisorThenKillAfterDelayAsync(
        TimeSpan delay,
        Dictionary<string, string> envOverrides,
        CancellationToken ct)
    {
        var psi = new ProcessStartInfo
        {
            FileName = _workerExePath,
            WorkingDirectory = _repoRoot,
            UseShellExecute = false
        };
        foreach (var a in _workerBaseArgs) psi.ArgumentList.Add(a);
        psi.ArgumentList.Add("supervisor");
        foreach (var (k, v) in envOverrides) psi.Environment[k] = v;

        using var proc = Process.Start(psi) ?? throw new InvalidOperationException("Failed to start supervisor.");
        await Task.Delay(delay, ct);

        if (!proc.HasExited)
        {
            Console.WriteLine("[chaos] killing supervisor + worker fleet mid-run to simulate sudden crash/redeploy...");
            proc.Kill(entireProcessTree: true);
        }

        await proc.WaitForExitAsync(ct);
    }

    private async Task RunExternalProcessAsync(
        string fileName,
        string[] args,
        Dictionary<string, string> envOverrides,
        CancellationToken ct)
    {
        var psi = new ProcessStartInfo
        {
            FileName = fileName,
            WorkingDirectory = _repoRoot,
            UseShellExecute = false
        };
        foreach (var a in args) psi.ArgumentList.Add(a);
        foreach (var (k, v) in envOverrides) psi.Environment[k] = v;

        using var proc = Process.Start(psi) ?? throw new InvalidOperationException($"Failed to start {fileName}.");
        await proc.WaitForExitAsync(ct);
        if (proc.ExitCode != 0)
        {
            throw new InvalidOperationException($"{fileName} {string.Join(' ', args)} exited with code {proc.ExitCode}.");
        }
    }
}
