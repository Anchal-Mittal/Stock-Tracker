using StockStream.Domain.ValueObjects;

namespace StockStream.Domain.Entities;

/// <summary>
/// Represents a single update message arrived in queue_messages for a specific store.
/// </summary>
public sealed record QueuedUpdate(
    long QueueRowId,
    Guid MessageId,
     int Seq,
    UpdateKind Kind,
    ReceiptAvailability ReceiptState,
    double? ReadyAtEpochMs,
    bool PayloadValid);

/// <summary>
/// Audit record for a sequence number that was intentionally skipped (lost or superseded by a later snapshot).
/// </summary>
public sealed record SkippedUpdate(
    int Seq,
    QueuedUpdate? Row,
    SkipReason Reason);

/// <summary>
/// Tracks when a missing sequence gap was first observed on the database clock.
/// </summary>
public readonly record struct SequenceGapTimer(int Seq, double SinceEpochMs);

/// <summary>
/// Non-fatal stream anomaly (e.g., two distinct message_ids claiming the same store-local sequence number).
/// </summary>
public sealed record StreamAnomaly(int Seq, string Detail);

/// <summary>
/// Consistent read-model snapshot of a single store's synchronization state and its pending queue messages.
/// </summary>
public sealed record StoreStreamSnapshot(
    StoreIdentity Store,
    int LastSeq,
    IReadOnlyList<QueuedUpdate> OutstandingRows,
    double NowEpochMs,
    SequenceGapTimer? ActiveGap,
    double GapTimeoutMs);
