using StockStream.Domain.Entities;
using StockStream.Domain.ValueObjects;

namespace StockStream.Domain.Ports;

public enum ApplyAbortReason
{
    None,
    CasConflict,
    ReceiptNotReady
}

public readonly record struct ApplyOutcome(bool Succeeded, int NewVersion, ApplyAbortReason AbortReason)
{
    public static ApplyOutcome Success(int newVersion) => new(true, newVersion, ApplyAbortReason.None);
    public static ApplyOutcome Aborted(ApplyAbortReason reason) => new(false, 0, reason);
}

public sealed record StoreCandidate(
    StoreIdentity Store,
    bool IsDue,
    double? NotBeforeEpochMs,
    string? CurrentStatus);

public sealed record CandidateBatch(
    IReadOnlyList<StoreCandidate> Candidates,
    double NowEpochMs);

public sealed record StoreSchedulingHint(
    StoreSyncStatus Status,
    double? NotBeforeEpochMs = null,
    int? GapSeq = null,
    int? BlockedSeq = null,
    string? BlockedReason = null,
    string? BlockedFingerprint = null);

public sealed record FleetSyncSummary(
    int StoresUpdated,
    int UpdatesApplied,
    int DistinctWorkersInLog,
    int UpdatesSkipped,
    int BlockedStores);

/// <summary>
/// Port for non-blocking per-store distributed coordination locks (ISP).
/// </summary>
public interface IStoreLeaseManager
{
    Task<bool> TryAcquireLeaseAsync(StoreIdentity store, CancellationToken ct = default);
    Task ReleaseLeaseAsync(StoreIdentity store, CancellationToken ct = default);
}

/// <summary>
/// Port for idempotent additive schema setup and hint cache resets (ISP).
/// </summary>
public interface ISchemaMigrator
{
    Task MigrateAsync(CancellationToken ct = default);
    Task ResetSchedulingHintsAsync(CancellationToken ct = default);
}

/// <summary>
/// Port for store stream state persistence and transactional updates (ISP + DIP).
/// </summary>
public interface IStoreSyncRepository
{
    Task<StoreStreamSnapshot> LoadStoreViewAsync(
        StoreIdentity store,
        double gapTimeoutMs,
        CancellationToken ct = default);

    Task<ApplyOutcome> ApplyUpdateAsync(
        StoreIdentity store,
        QueuedUpdate row,
        int expectedLastSeq,
        IReadOnlyList<SkippedUpdate> skipped,
        string workerId,
        Action? midTransactionHook = null,
        CancellationToken ct = default);

    Task RecordSkippedUpdatesAsync(
        StoreIdentity store,
        IReadOnlyList<SkippedUpdate> skipped,
        int? resolvedBySeq,
        string workerId,
        CancellationToken ct = default);

    Task<string> ComputeStoreFingerprintAsync(StoreIdentity store, CancellationToken ct = default);

    Task UpdateSchedulingHintAsync(
        StoreIdentity store,
        string workerId,
        StoreSchedulingHint hint,
        CancellationToken ct = default);

    Task<CandidateBatch> FindCandidatesAsync(CancellationToken ct = default);

    Task<string> ComputeQueueSignatureAsync(CancellationToken ct = default);

    Task<FleetSyncSummary> GetFleetSummaryAsync(CancellationToken ct = default);
}

/// <summary>
/// Port for structured per-process diagnostic logging (ISP).
/// </summary>
public interface ISyncLogger
{
    void Debug(string message, object? fields = null);
    void Info(string message, object? fields = null);
    void Warn(string message, object? fields = null);
    void Error(string message, object? fields = null);
}
