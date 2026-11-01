using StockStream.Application.Configuration;
using StockStream.Domain.Ports;

namespace StockStream.Application.Services;

public sealed record WorkerRunStatistics(
    string WorkerId,
    int TotalApplied,
    int TotalSkipped,
    int BlockedStoreCount);

/// <summary>
/// Application Service (SRP): drives a single OS worker process's poll-claim-advance-drain loop.
/// </summary>
public sealed class WorkerEngine
{
    private readonly ISchemaMigrator _migrator;
    private readonly IStoreLeaseManager _leaseManager;
    private readonly IStoreSyncRepository _repository;
    private readonly StoreBacklogAdvancer _advancer;
    private readonly ISyncLogger _logger;
    private readonly ProcessorOptions _options;

    public WorkerEngine(
        ISchemaMigrator migrator,
        IStoreLeaseManager leaseManager,
        IStoreSyncRepository repository,
        StoreBacklogAdvancer advancer,
        ISyncLogger logger,
        ProcessorOptions options)
    {
        _migrator = migrator ?? throw new ArgumentNullException(nameof(migrator));
        _leaseManager = leaseManager ?? throw new ArgumentNullException(nameof(leaseManager));
        _repository = repository ?? throw new ArgumentNullException(nameof(repository));
        _advancer = advancer ?? throw new ArgumentNullException(nameof(advancer));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    public async Task<WorkerRunStatistics> RunAsync(string workerId, string slot, CancellationToken ct = default)
    {
        await _migrator.MigrateAsync(ct);
        _logger.Info("worker online", new { workerId, slot });

        var totalApplied = 0;
        var totalSkipped = 0;
        var blockedStores = 0;

        long? emptySinceTickMs = null;
        string? emptySignature = null;

        while (!ct.IsCancellationRequested)
        {
            var batch = await _repository.FindCandidatesAsync(ct);

            if (batch.Candidates.Count == 0)
            {
                var signature = await _repository.ComputeQueueSignatureAsync(ct);
                var nowTick = Environment.TickCount64;

                if (emptySinceTickMs is null || emptySignature != signature)
                {
                    emptySinceTickMs = nowTick;
                    emptySignature = signature;
                }
                else if (nowTick - emptySinceTickMs.Value >= _options.DrainQuietMs)
                {
                    _logger.Info("queue drained — exiting cleanly", new
                    {
                        applied = totalApplied,
                        skipped = totalSkipped,
                        blockedStores
                    });
                    break;
                }

                var quietCheckSleep = Math.Clamp(_options.DrainQuietMs / 3, _options.MinIdleSleepMs, _options.MaxIdleSleepMs);
                await Task.Delay(quietCheckSleep, ct).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
                continue;
            }

            emptySinceTickMs = null;
            emptySignature = null;

            var dueCandidates = batch.Candidates.Where(c => c.IsDue).ToArray();
            Random.Shared.Shuffle(dueCandidates);

            var progressed = false;

            foreach (var candidate in dueCandidates)
            {
                if (ct.IsCancellationRequested) break;

                if (!await _leaseManager.TryAcquireLeaseAsync(candidate.Store, ct))
                {
                    continue;
                }

                try
                {
                    var outcome = await _advancer.AdvanceStoreAsync(candidate.Store, workerId, ct);
                    totalApplied += outcome.AppliedCount;
                    totalSkipped += outcome.SkippedCount;
                    if (outcome.BecameBlocked) blockedStores += 1;
                    if (outcome.AppliedCount > 0 || outcome.StateChanged) progressed = true;
                }
                finally
                {
                    try
                    {
                        await _leaseManager.ReleaseLeaseAsync(candidate.Store, CancellationToken.None);
                    }
                    catch (Exception ex)
                    {
                        _logger.Warn("failed to release store advisory lock", new
                        {
                            storeId = candidate.Store.StoreId,
                            err = ex.Message
                        });
                    }
                }
            }

            if (ct.IsCancellationRequested) break;
            if (progressed) continue;

            var sleepMs = ComputeIdleSleepMs(batch);
            await Task.Delay(sleepMs, ct).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        }

        return new WorkerRunStatistics(workerId, totalApplied, totalSkipped, blockedStores);
    }

    private int ComputeIdleSleepMs(CandidateBatch batch)
    {
        var earliestWaitMs = (double)_options.MaxIdleSleepMs;
        foreach (var c in batch.Candidates)
        {
            if (!c.IsDue && c.NotBeforeEpochMs is { } nb)
            {
                var delta = Math.Ceiling(nb - batch.NowEpochMs);
                if (delta < earliestWaitMs) earliestWaitMs = delta;
            }
        }

        return (int)Math.Clamp(earliestWaitMs, _options.MinIdleSleepMs, _options.MaxIdleSleepMs);
    }
}
