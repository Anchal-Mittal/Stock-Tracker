using System.Text.Json;
using StockStream.Application.Configuration;
using StockStream.Application.Logging;
using StockStream.Application.Services;
using StockStream.Cli.Verification;
using StockStream.Domain.Services;
using StockStream.Infrastructure.Locking;
using StockStream.Infrastructure.Persistence;

namespace StockStream.Cli;

public static class Program
{
    private static readonly JsonSerializerOptions PrettyJson = new() { WriteIndented = true };

    public static async Task<int> Main(string[] args)
    {
        var mode = args.Length > 0 ? args[0].ToLowerInvariant() : "supervisor";
        var options = ProcessorOptions.FromEnvironment();

        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };
        AppDomain.CurrentDomain.ProcessExit += (_, _) =>
        {
            try { cts.Cancel(); } catch { /* already disposed */ }
        };

        try
        {
            return mode switch
            {
                "supervisor" => await RunSupervisorModeAsync(options, cts.Token),
                "worker" => await RunWorkerModeAsync(options, cts.Token),
                "verify" => await RunVerifyModeAsync(options, cts.Token),
                "chaos" => await RunChaosModeAsync(options, cts.Token),
                _ => throw new ArgumentException($"Unknown command '{mode}'. Expected: supervisor | worker | verify | chaos.")
            };
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested)
        {
            return 0;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[{mode}:{Environment.ProcessId}] fatal error: {ex}");
            return 1;
        }
    }

    private static async Task<int> RunSupervisorModeAsync(ProcessorOptions options, CancellationToken ct)
    {
        var logger = new ConsoleSyncLogger("supervisor", options.LogLevel);
        await using var session = await PostgresSession.ConnectWithRetryAsync(
            options.DatabaseUrl,
            $"stockstream-supervisor-{Environment.ProcessId}",
            logger,
            ct: ct);

        var migrator = new PostgresSchemaMigrator(session);
        var repository = new PostgresStoreSyncRepository(session);
        var (exePath, baseArgs) = ResolveSelfProcessInvocation();

        var supervisor = new FleetSupervisor(
            migrator,
            repository,
            logger,
            options,
            exePath,
            baseArgs);

        await supervisor.RunFleetAsync(ct);
        return 0;
    }

    private static async Task<int> RunWorkerModeAsync(ProcessorOptions options, CancellationToken ct)
    {
        var workerId = Environment.ProcessId.ToString();
        var slot = Environment.GetEnvironmentVariable("WORKER_SLOT") ?? "?";
        var logger = new ConsoleSyncLogger($"worker#{slot}", options.LogLevel);

        await using var session = await PostgresSession.ConnectWithRetryAsync(
            options.DatabaseUrl,
            $"stockstream-worker-{slot}-{workerId}",
            logger,
            ct: ct);

        var migrator = new PostgresSchemaMigrator(session);
        var leaseManager = new PostgresAdvisoryLeaseManager(session);
        var repository = new PostgresStoreSyncRepository(session);
        var planner = new StoreStreamPlanner();
        var advancer = new StoreBacklogAdvancer(planner, repository, logger, options);
        var engine = new WorkerEngine(migrator, leaseManager, repository, advancer, logger, options);

        await engine.RunAsync(workerId, slot, ct);
        return 0;
    }

    private static async Task<int> RunVerifyModeAsync(ProcessorOptions options, CancellationToken ct)
    {
        var verifier = new GroundTruthVerifier(options.DatabaseUrl);
        var report = await verifier.VerifyAsync(ct);
        Console.WriteLine(JsonSerializer.Serialize(report, PrettyJson));

        if (!report.Ok)
        {
            Console.Error.WriteLine($"\nVERIFICATION FAILED with {report.Violations.Count} violation(s).");
            return 1;
        }

        var duplicatesIgnored = report.TotalQueueRows - report.DistinctLogicalUpdates;
        Console.WriteLine(
            $"\nVERIFICATION PASSED: {report.StoresChecked} stores, {report.TotalApplied} applied ({duplicatesIgnored} duplicates ignored, {report.TotalSkipped} skipped) across {report.DistinctWorkers.Count} worker PIDs.");
        return 0;
    }

    private static async Task<int> RunChaosModeAsync(ProcessorOptions options, CancellationToken ct)
    {
        var (exePath, baseArgs) = ResolveSelfProcessInvocation();
        var repoRoot = FindRepoRoot();
        var runner = new ChaosSuiteRunner(options, repoRoot, exePath, baseArgs);
        await runner.RunAllScenariosAsync(ct);
        return 0;
    }

    private static (string ExePath, string[] BaseArgs) ResolveSelfProcessInvocation()
    {
        var processPath = Environment.ProcessPath
            ?? throw new InvalidOperationException("Unable to determine current executable path.");
        var exeName = Path.GetFileNameWithoutExtension(processPath);

        if (string.Equals(exeName, "dotnet", StringComparison.OrdinalIgnoreCase))
        {
            var dllPath = Path.Combine(AppContext.BaseDirectory, "StockStream.Cli.dll");
            return (processPath, [dllPath]);
        }

        return (processPath, []);
    }

    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(Directory.GetCurrentDirectory());
        while (dir is not null)
        {
            if (File.Exists(Path.Combine(dir.FullName, "seed.js")))
            {
                return dir.FullName;
            }
            dir = dir.Parent;
        }
        return Directory.GetCurrentDirectory();
    }
}
