namespace StockStream.Domain.ValueObjects;

/// <summary>
/// Value Object uniquely identifying a Store within a Retail Chain.
/// </summary>
public readonly record struct StoreIdentity(Guid ChainId, Guid StoreId)
{
    /// <summary>
    /// Deterministic composite key used for PostgreSQL 64-bit session advisory locks.
    /// </summary>
    public string ToCompositeKey() => $"{ChainId:D}/{StoreId:D}";

    public override string ToString() => ToCompositeKey();
}
