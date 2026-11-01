using System.Diagnostics;
using StockStream.Application.Configuration;
using StockStream.Domain.Ports;

namespace StockStream.Application.Services;

/// <summary>
/// Application Service (SRP): manages the fleet of 4 independent OS worker processes.
/// Workers share NOTHING except PostgreSQL (no IPC, no shared memory, no local files).
/// </summary>
public sealed class FleetSupervisor
{
    private readonly ISchemaMigrator _migrator;
    private readonly IStoreSyncRepository _repository;
    private readonly ISyncLogger _logger;
    private readonly ProcessorOptions _options;
    private readonly string _workerExecutablePath;
    private readonly string[] _workerBaseArguments;

    public FleetSupervisor(
        ISchemaMigrator migrator,
        IStoreSyncRepository repository,
        ISyncLogger logger,
        ProcessorOptions options,
        string workerExecutablePath,
        string[] workerBaseArguments)
    {
        _migrator = migrator ?? throw new ArgumentNullException(nameof(migrator));
        _repository = repository ?? throw new ArgumentNullException(nameof(repository));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _workerExecutablePath = workerExecutablePath;
        _workerBaseArguments = workerBaseArguments;
    }

    public async Task RunFleetAsync(CancellationToken ct = default)
    {
        var sw = Stopwatch.StartNew();

        // 1. Run additive migrations and reset transient scheduling hints before spawning the 4 workers.
        await _migrator.MigrateAsync(ct);
        await _migrator.ResetSchedulingHintsAsync(ct);

        var slots = Enumerable.Range(1, _options.WorkerCount)
            .Select(slotNum => new WorkerSlotState(slotNum))
            .ToArray();

        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        Exception? fatalError = null;
        var syncLock = new object();
        var allCompletedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        void CheckAllCompleted()
        {
            lock (syncLock)
            {
                if (fatalError is not null)
                {
                    allCompletedTcs.TrySetException(fatalError);
                    return;
                }

                if (slots.All(s => s.IsCompleted))
                {
                    allCompletedTcs.TrySetResult();
                }
            }
        }

        void SpawnWorkerForSlot(WorkerSlotState slot)
        {
            lock (syncLock)
            {
                if (linkedCts.IsCancellationRequested || fatalError is not null)
                {
                    slot.IsCompleted = true;
                    CheckAllCompleted();
                    return;
                }

                var psi = new ProcessStartInfo
                {
                    FileName = _workerExecutablePath,
                    UseShellExecute = false,
                    RedirectStandardInput = false,
                    RedirectStandardOutput = false,
                    RedirectStandardError = false
                };

                foreach (var arg in _workerBaseArguments)
                {
                    psi.ArgumentList.Add(arg);
                }
                psi.ArgumentList.Add("worker");
                psi.Environment["WORKER_SLOT"] = slot.SlotNumber.ToString();

                var proc = new Process
                {
                    StartInfo = psi,
                    EnableRaisingEvents = true
                };

                proc.Exited += (_, _) =>
                {
                    int exitCode;
                    int deadPid;
                    try
                    {
                        exitCode = proc.ExitCode;
                        deadPid = proc.Id;
                    }
                    catch
                    {
                        exitCode = -1;
                        deadPid = -1;
                    }
                    finally
                    {
                        proc.Dispose();
                    }

                    lock (syncLock)
                    {
                        slot.ActiveProcess = null;

                        if (exitCode == 0)
                        {
                            slot.IsCompleted = true;
                            _logger.Info("worker exited cleanly", new
                            {
                                slot = slot.SlotNumber,
                                workerPid = deadPid,
                                remainingWorkers = slots.Count(s => !s.IsCompleted)
                            });
                            CheckAllCompleted();
                            return;
                        }

                        if (linkedCts.IsCancellationRequested)
                        {
                            slot.IsCompleted = true;
                            CheckAllCompleted();
                            return;
                        }

                        slot.RestartCount += 1;
                        if (slot.RestartCount > _options.MaxRestartsPerSlot)
                        {
                            fatalError = new InvalidOperationException(
                                $"Worker slot #{slot.SlotNumber} exceeded max restarts ({_options.MaxRestartsPerSlot}); last exitCode={exitCode}.");
                            _logger.Error(fatalError.Message);
                            linkedCts.Cancel();
                            slot.IsCompleted = true;
                            CheckAllCompleted();
                            return;
                        }

                        _logger.Warn("worker exited abnormally — respawning replacement", new
                        {
                            slot = slot.SlotNumber,
                            deadPid,
                            exitCode,
                            restartAttempt = slot.RestartCount
                        });

                        _ = Task.Run(async () =>
                        {
                            await Task.Delay(_options.RestartBackoffMs, linkedCts.Token)
                                .ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
                            SpawnWorkerForSlot(slot);
                        });
                    }
                };

                if (!proc.Start())
                {
                    fatalError = new InvalidOperationException($"Failed to start OS worker process for slot #{slot.SlotNumber}.");
                    slot.IsCompleted = true;
                    CheckAllCompleted();
                    return;
                }

                slot.ActiveProcess = proc;
                _logger.Info("spawned worker process", new
                {
                    slot = slot.SlotNumber,
                    workerPid = proc.Id,
                    restarts = slot.RestartCount
                });
            }
        }

        using var reg = ct.Register(() =>
        {
            lock (syncLock)
            {
                foreach (var s in slots)
                {
                    try
                    {
                        if (s.ActiveProcess is { HasExited: false } p)
                        {
                            p.Kill(entireProcessTree: true);
                        }
                    }
                    catch
                    {
                        // Process already exited.
                    }
                }
            }
        });

        foreach (var slot in slots)
        {
            SpawnWorkerForSlot(slot);
        }

        await allCompletedTcs.Task;

        var summary = await _repository.GetFleetSummaryAsync(CancellationToken.None);
        _logger.Info("all workers finished — backlog sync complete", new
        {
            elapsedMs = sw.ElapsedMilliseconds,
            storesUpdated = summary.StoresUpdated,
            updatesApplied = summary.UpdatesApplied,
            distinctWorkersInLog = summary.DistinctWorkersInLog,
            updatesSkipped = summary.UpdatesSkipped,
            blockedStores = summary.BlockedStores,
            totalRestarts = slots.Sum(s => s.RestartCount)
        });
    }

    private sealed class WorkerSlotState(int slotNumber)
    {
        public int SlotNumber { get; } = slotNumber;
        public Process? ActiveProcess { get; set; }
        public int RestartCount { get; set; }
        public bool IsCompleted { get; set; }
    }
}
