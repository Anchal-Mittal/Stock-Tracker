using StockStream.Domain.Entities;
using StockStream.Domain.ValueObjects;

namespace StockStream.Domain.Decisions;

/// <summary>
/// Closed discriminated-union hierarchy representing the next deterministic action for a store stream.
/// </summary>
public abstract record SyncDecision
{
    private SyncDecision() { }

    /// <summary>
    /// Apply <see cref="Row"/> atomically inside a compare-and-set transaction expecting <see cref="ExpectedLastSeq"/>.
    /// </summary>
    public sealed record ApplyUpdateDecision(
        QueuedUpdate Row,
        int ExpectedLastSeq,
        IReadOnlyList<SkippedUpdate> Skipped) : SyncDecision;

    /// <summary>
    /// Hold the store stream at <see cref="Seq"/> until its receipt upload matures at <see cref="UntilEpochMs"/>.
    /// </summary>
    public sealed record WaitForReceiptDecision(
        int Seq,
        double UntilEpochMs) : SyncDecision;

    /// <summary>
    /// Hold the store stream while waiting for missing sequence <see cref="Seq"/> within the gap timeout window.
    /// </summary>
    public sealed record WaitForMissingSequenceDecision(int Seq) : SyncDecision;

    /// <summary>
    /// Mark the store blocked because <see cref="Seq"/> is permanently unprocessable and no later snapshot exists.
    /// </summary>
    public sealed record BlockStoreDecision(
        int Seq,
        SkipReason Reason,
        IReadOnlyList<SkippedUpdate> Skipped) : SyncDecision;

    /// <summary>
    /// All known updates for this store have been applied.
    /// </summary>
    public sealed record StoreDrainedDecision : SyncDecision
    {
        public static readonly StoreDrainedDecision Instance = new();
    }
}

public sealed record StorePlanResult(
    SyncDecision Decision,
    IReadOnlyList<StreamAnomaly> Anomalies);
