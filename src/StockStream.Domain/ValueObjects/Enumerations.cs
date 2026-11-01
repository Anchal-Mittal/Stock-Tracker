namespace StockStream.Domain.ValueObjects;

public enum UpdateKind
{
    Snapshot,
    Delta
}

public enum ReceiptAvailability
{
    Ready,
    Pending,
    PermanentlyFailed
}

public enum SkipReason
{
    ReceiptFailed,
    InvalidPayload,
    DeltaWithoutBase,
    MissingSequence,
    SupersededAfterLoss,
    BlockedBehindLostSequence
}

public enum StoreSyncStatus
{
    Waiting,
    Gap,
    Blocked,
    Drained
}

public static class DomainEnumMappings
{
    public static UpdateKind ParseUpdateKind(string raw) => raw switch
    {
        "snapshot" => UpdateKind.Snapshot,
        "delta" => UpdateKind.Delta,
        _ => throw new ArgumentOutOfRangeException(nameof(raw), raw, "Unknown update kind.")
    };

    public static string ToDatabaseValue(this UpdateKind kind) => kind switch
    {
        UpdateKind.Snapshot => "snapshot",
        UpdateKind.Delta => "delta",
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, null)
    };

    public static ReceiptAvailability ParseReceiptAvailability(string raw) => raw switch
    {
        "ready" => ReceiptAvailability.Ready,
        "pending" => ReceiptAvailability.Pending,
        "failed" => ReceiptAvailability.PermanentlyFailed,
        _ => throw new ArgumentOutOfRangeException(nameof(raw), raw, "Unknown receipt state.")
    };

    public static string ToDatabaseValue(this SkipReason reason) => reason switch
    {
        SkipReason.ReceiptFailed => "receipt_failed",
        SkipReason.InvalidPayload => "invalid_payload",
        SkipReason.DeltaWithoutBase => "delta_without_base",
        SkipReason.MissingSequence => "missing_seq",
        SkipReason.SupersededAfterLoss => "superseded_after_loss",
        SkipReason.BlockedBehindLostSequence => "blocked_behind_lost_seq",
        _ => throw new ArgumentOutOfRangeException(nameof(reason), reason, null)
    };

    public static string ToDatabaseValue(this StoreSyncStatus status) => status switch
    {
        StoreSyncStatus.Waiting => "waiting",
        StoreSyncStatus.Gap => "gap",
        StoreSyncStatus.Blocked => "blocked",
        StoreSyncStatus.Drained => "drained",
        _ => throw new ArgumentOutOfRangeException(nameof(status), status, null)
    };
}
