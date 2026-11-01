using System.Diagnostics;
using StockStream.Application.Configuration;
using StockStream.Domain.Decisions;
using StockStream.Domain.Ports;
using StockStream.Domain.Services;
using StockStream.Domain.ValueObjects;

namespace StockStream.Application.Services;

public readonly record struct StoreAdvanceOutcome(
    int AppliedCount,
    int SkippedCount,
    bool BecameBlocked,
    bool StateChanged);

/// <summary>
/// Application Use Case (SRP): advances a single leased store stream by coordinating
/// the pure domain planner (<see cref="IStoreStreamPlanner"/>) with the repository port
/// (<see cref="IStoreSyncRepository"/>).
/// </summary>
public sealed class StoreBacklogAdvancer
{
    private readonly IStoreStreamPlanner _planner;
    private readonly IStoreSyncRepository _repository;
    private readonly ISyncLogger _logger;
    private readonly ProcessorOptions _options;

    public StoreBacklogAdvancer(
        IStoreStreamPlanner planner,
        IStoreSyncRepository repository,
        ISyncLogger logger,
        ProcessorOptions options)
    {
        _planner = planner ?? throw new ArgumentNullException(nameof(planner));
        _repository = repository ?? throw new ArgumentNullException(nameof(repository));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    public async Task<StoreAdvanceOutcome> AdvanceStoreAsync(
        StoreIdentity store,
        string workerId,
        CancellationToken ct = default)
    {
        var applied = 0;
        var skipped = 0;
        var blocked = false;
        var stateChanged = false;

        var view = await _repository.LoadStoreViewAsync(store, _options.GapTimeoutMs, ct);
        var dirtyView = false;

        while (!ct.IsCancellationRequested && applied < _options.MaxAppliesPerClaim)
        {
            var plan = _planner.PlanNext(view);

            // If we advanced `view` in-memory and hit a non-apply decision, reload from
            // PostgreSQL once so we observe the fresh DB clock and any newly arrived rows.
            if (plan.Decision is not SyncDecision.ApplyUpdateDecision && dirtyView)
            {
                view = await _repository.LoadStoreViewAsync(store, _options.GapTimeoutMs, ct);
                dirtyView = false;
                plan = _planner.PlanNext(view);
            }

            foreach (var anomaly in plan.Anomalies)
            {
                _logger.Warn("store sequence anomaly detected", new
                {
                    chainId = store.ChainId,
                    storeId = store.StoreId,
                    seq = anomaly.Seq,
                    detail = anomaly.Detail
                });
            }

            switch (plan.Decision)
            {
                case SyncDecision.ApplyUpdateDecision applyDecision:
                {
                    var outcome = await _repository.ApplyUpdateAsync(
                        store,
                        applyDecision.Row,
                        applyDecision.ExpectedLastSeq,
                        applyDecision.Skipped,
                        workerId,
                        midTransactionHook: () => MaybeInjectChaosCrash(store, applyDecision.Row.Seq),
                        ct: ct);

                    if (!outcome.Succeeded)
                    {
                        _logger.Debug("apply aborted by transactional guard, reloading store view", new
                        {
                            storeId = store.StoreId,
                            seq = applyDecision.Row.Seq,
                            reason = outcome.AbortReason.ToString()
                        });
                        view = await _repository.LoadStoreViewAsync(store, _options.GapTimeoutMs, ct);
                        dirtyView = false;
                        continue;
                    }

                    applied += 1;
                    skipped += applyDecision.Skipped.Count;

                    if (applyDecision.Skipped.Count > 0)
                    {
                        _logger.Warn("recovered store via snapshot after skipping unprocessable updates", new
                        {
                            storeId = store.StoreId,
                            recoverySnapshotSeq = applyDecision.Row.Seq,
                            skippedSeqs = applyDecision.Skipped.Select(s => new { seq = s.Seq, reason = s.Reason.ToDatabaseValue() })
                        });
                    }

                    // Advance in-memory view to apply consecutive ready updates without redundant SELECTs.
                    view = view with
                    {
                        LastSeq = applyDecision.Row.Seq,
                        ActiveGap = null
                    };
                    dirtyView = true;
                    continue;
                }

                case SyncDecision.StoreDrainedDecision:
                {
                    await _repository.UpdateSchedulingHintAsync(
                        store,
                        workerId,
                        new StoreSchedulingHint(StoreSyncStatus.Drained),
                        ct);
                    stateChanged = true;
                    goto DoneLoop;
                }

                case SyncDecision.WaitForReceiptDecision waitDecision:
                {
                    await _repository.UpdateSchedulingHintAsync(
                        store,
                        workerId,
                        new StoreSchedulingHint(
                            StoreSyncStatus.Waiting,
                            NotBeforeEpochMs: waitDecision.UntilEpochMs,
                            GapSeq: null),
                        ct);
                    _logger.Debug("store waiting for delayed receipt", new
                    {
                        storeId = store.StoreId,
                        seq = waitDecision.Seq,
                        waitMs = Math.Max(0, (long)Math.Round(waitDecision.UntilEpochMs - view.NowEpochMs))
                    });
                    stateChanged = true;
                    goto DoneLoop;
                }

                case SyncDecision.WaitForMissingSequenceDecision gapDecision:
                {
                    var firstSeenMs = view.ActiveGap is { } g && g.Seq == gapDecision.Seq
                        ? g.SinceEpochMs
                        : view.NowEpochMs;
                    var expireAtMs = firstSeenMs + _options.GapTimeoutMs;
                    var nextCheckMs = Math.Min(expireAtMs, view.NowEpochMs + _options.MaxIdleSleepMs);

                    await _repository.UpdateSchedulingHintAsync(
                        store,
                        workerId,
                        new StoreSchedulingHint(
                            StoreSyncStatus.Gap,
                            NotBeforeEpochMs: nextCheckMs,
                            GapSeq: gapDecision.Seq),
                        ct);
                    _logger.Warn("missing sequence number — waiting up to gap timeout before failing over", new
                    {
                        storeId = store.StoreId,
                        missingSeq = gapDecision.Seq
                    });
                    stateChanged = true;
                    goto DoneLoop;
                }

                case SyncDecision.BlockStoreDecision blockDecision:
                {
                    await _repository.RecordSkippedUpdatesAsync(
                        store,
                        blockDecision.Skipped,
                        resolvedBySeq: null,
                        workerId,
                        ct);
                    var fingerprint = await _repository.ComputeStoreFingerprintAsync(store, ct);
                    await _repository.UpdateSchedulingHintAsync(
                        store,
                        workerId,
                        new StoreSchedulingHint(
                            StoreSyncStatus.Blocked,
                            NotBeforeEpochMs: null,
                            GapSeq: null,
                            BlockedSeq: blockDecision.Seq,
                            BlockedReason: blockDecision.Reason.ToDatabaseValue(),
                            BlockedFingerprint: fingerprint),
                        ct);
                    skipped += blockDecision.Skipped.Count;
                    blocked = true;
                    stateChanged = true;
                    _logger.Warn("store permanently blocked (no subsequent snapshot to recover)", new
                    {
                        chainId = store.ChainId,
                        storeId = store.StoreId,
                        blockedAtSeq = blockDecision.Seq,
                        reason = blockDecision.Reason.ToDatabaseValue(),
                        unappliedCount = blockDecision.Skipped.Count
                    });
                    goto DoneLoop;
                }
            }
        }

    DoneLoop:
        if (applied >= _options.MaxAppliesPerClaim)
        {
            await _repository.UpdateSchedulingHintAsync(
                store,
                workerId,
                new StoreSchedulingHint(StoreSyncStatus.Waiting, NotBeforeEpochMs: null, GapSeq: null),
                ct);
        }

        return new StoreAdvanceOutcome(applied, skipped, blocked, stateChanged);
    }

    private void MaybeInjectChaosCrash(StoreIdentity store, int seq)
    {
        if (_options.ChaosCrashProb > 0.0 && Random.Shared.NextDouble() < _options.ChaosCrashProb)
        {
            _logger.Warn("CHAOS: simulating hard crash mid-transaction before COMMIT", new
            {
                storeId = store.StoreId,
                seq
            });
            Process.GetCurrentProcess().Kill();
        }
    }
}
