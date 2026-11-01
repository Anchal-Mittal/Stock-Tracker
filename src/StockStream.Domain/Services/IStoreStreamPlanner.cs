using StockStream.Domain.Decisions;
using StockStream.Domain.Entities;

namespace StockStream.Domain.Services;

/// <summary>
/// Pure domain service that computes the next synchronization decision for a store stream.
/// </summary>
public interface IStoreStreamPlanner
{
    StorePlanResult PlanNext(StoreStreamSnapshot view);
}
