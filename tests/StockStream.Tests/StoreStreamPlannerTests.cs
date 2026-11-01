using StockStream.Application.Configuration;
using StockStream.Application.Services;
using StockStream.Domain.Decisions;
using StockStream.Domain.Entities;
using StockStream.Domain.Ports;
using StockStream.Domain.Services;
using StockStream.Domain.ValueObjects;
using Xunit;

namespace StockStream.Tests;

public sealed class StoreStreamPlannerTests
{
    private static readonly StoreIdentity SampleStore = new(
        Guid.Parse("11111111-1111-4111-8111-111111111111"),
        Guid.Parse("22222222-2222-4222-8222-222222222222"));

    private readonly StoreStreamPlanner _planner = new();

    private static QueuedUpdate Row(
        int seq,
        long? id = null,
        Guid? messageId = null,
        UpdateKind? kind = null,
        ReceiptAvailability receiptState = ReceiptAvailability.Ready,
        double? readyAtMs = 1000.0,
        bool payloadValid = true)
    {
        return new QueuedUpdate(
            QueueRowId: id ?? (seq * 10L),
            MessageId: messageId ?? Guid.Parse($"00000000-0000-4000-8000-{seq:D12}"),
            Seq: seq,
            Kind: kind ?? (seq == 1 ? UpdateKind.Snapshot : UpdateKind.Delta),
            ReceiptState: receiptState,
            ReadyAtEpochMs: receiptState == ReceiptAvailability.PermanentlyFailed ? null : readyAtMs,
            PayloadValid: payloadValid);
    }

    private static StoreStreamSnapshot View(
        IReadOnlyList<QueuedUpdate> rows,
        int lastSeq = 0,
        double nowMs = 10_000.0,
        SequenceGapTimer? gap = null,
        double gapTimeoutMs = 60_000.0)
    {
        return new StoreStreamSnapshot(SampleStore, lastSeq, rows, nowMs, gap, gapTimeoutMs);
    }

    [Fact]
    public void PlanNext_ReturnsDrained_WhenNoRowsExceedLastSeq()
    {
        var result = _planner.PlanNext(View([Row(1), Row(2), Row(3)], lastSeq: 3));

        Assert.IsType<SyncDecision.StoreDrainedDecision>(result.Decision);
        Assert.Empty(result.Anomalies);
    }

    [Fact]
    public void PlanNext_OrdersShuffledArrivalsStrictlyBySeqStartingAtLastSeqPlusOne()
    {
        var rows = new[] { Row(3, id: 30), Row(1, id: 10, kind: UpdateKind.Snapshot), Row(2, id: 20) };

        var p1 = _planner.PlanNext(View(rows, lastSeq: 0));
        var apply1 = Assert.IsType<SyncDecision.ApplyUpdateDecision>(p1.Decision);
        Assert.Equal(1, apply1.Row.Seq);
        Assert.Equal(0, apply1.ExpectedLastSeq);
        Assert.Empty(apply1.Skipped);

        var p2 = _planner.PlanNext(View(rows, lastSeq: 1));
        var apply2 = Assert.IsType<SyncDecision.ApplyUpdateDecision>(p2.Decision);
        Assert.Equal(2, apply2.Row.Seq);
        Assert.Equal(1, apply2.ExpectedLastSeq);
    }

    [Fact]
    public void PlanNext_CollapsesDuplicateRowsWithSameMessageId_AndSelectsEarliestArrival()
    {
        var msgId = Guid.NewGuid();
        var rows = new[]
        {
            Row(1, id: 99, messageId: msgId, kind: UpdateKind.Snapshot),
            Row(1, id: 12, messageId: msgId, kind: UpdateKind.Snapshot)
        };

        var plan = _planner.PlanNext(View(rows, lastSeq: 0));
        var apply = Assert.IsType<SyncDecision.ApplyUpdateDecision>(plan.Decision);

        Assert.Equal(12L, apply.Row.QueueRowId);
        Assert.Empty(plan.Anomalies);
    }

    [Fact]
    public void PlanNext_FlagsConflictingMessageIdsAtSameSeqAsAnomaly_AndSelectsEarliestArrival()
    {
        var firstMsg = Guid.Parse("aaaaaaaa-0000-4000-8000-000000000001");
        var lateMsg = Guid.Parse("bbbbbbbb-0000-4000-8000-000000000002");
        var rows = new[]
        {
            Row(2, id: 50, messageId: lateMsg),
            Row(2, id: 15, messageId: firstMsg)
        };

        var plan = _planner.PlanNext(View(rows, lastSeq: 1));
        var apply = Assert.IsType<SyncDecision.ApplyUpdateDecision>(plan.Decision);

        Assert.Equal(firstMsg, apply.Row.MessageId);
        var anomaly = Assert.Single(plan.Anomalies);
        Assert.Contains("conflicting message_ids", anomaly.Detail);
    }

    [Fact]
    public void PlanNext_WaitsOnPendingReceipt_AndNeverAllowsLaterReadyUpdatesToOvertakeIt()
    {
        var rows = new[]
        {
            Row(2, receiptState: ReceiptAvailability.Pending, readyAtMs: 25_000.0),
            Row(3, receiptState: ReceiptAvailability.Ready, readyAtMs: 5_000.0),
            Row(4, kind: UpdateKind.Snapshot, receiptState: ReceiptAvailability.Ready, readyAtMs: 5_000.0)
        };

        var plan = _planner.PlanNext(View(rows, lastSeq: 1, nowMs: 10_000.0));
        var wait = Assert.IsType<SyncDecision.WaitForReceiptDecision>(plan.Decision);

        Assert.Equal(2, wait.Seq);
        Assert.Equal(25_000.0, wait.UntilEpochMs);
    }

    [Fact]
    public void PlanNext_SkipsPoisonedDeltaAndIntermediateDeltas_UpToNextRecoverySnapshot()
    {
        var rows = new[]
        {
            Row(2, receiptState: ReceiptAvailability.PermanentlyFailed),
            Row(3, kind: UpdateKind.Delta, receiptState: ReceiptAvailability.Ready),
            Row(4, kind: UpdateKind.Delta, receiptState: ReceiptAvailability.Ready),
            Row(5, kind: UpdateKind.Snapshot, receiptState: ReceiptAvailability.Ready),
            Row(6, kind: UpdateKind.Delta, receiptState: ReceiptAvailability.Ready)
        };

        var plan = _planner.PlanNext(View(rows, lastSeq: 1));
        var apply = Assert.IsType<SyncDecision.ApplyUpdateDecision>(plan.Decision);

        Assert.Equal(5, apply.Row.Seq);
        Assert.Equal(UpdateKind.Snapshot, apply.Row.Kind);
        Assert.Equal(1, apply.ExpectedLastSeq);
        Assert.Equal(
            new[]
            {
                (2, SkipReason.ReceiptFailed),
                (3, SkipReason.SupersededAfterLoss),
                (4, SkipReason.SupersededAfterLoss)
            },
            apply.Skipped.Select(s => (s.Seq, s.Reason)).ToArray());
    }

    [Fact]
    public void PlanNext_WaitsIfRecoverySnapshotAfterPoisonHasPendingReceipt()
    {
        var rows = new[]
        {
            Row(2, receiptState: ReceiptAvailability.PermanentlyFailed),
            Row(3, kind: UpdateKind.Delta, receiptState: ReceiptAvailability.Ready),
            Row(4, kind: UpdateKind.Snapshot, receiptState: ReceiptAvailability.Pending, readyAtMs: 30_000.0)
        };

        var plan = _planner.PlanNext(View(rows, lastSeq: 1, nowMs: 10_000.0));
        var wait = Assert.IsType<SyncDecision.WaitForReceiptDecision>(plan.Decision);

        Assert.Equal(4, wait.Seq);
        Assert.Equal(30_000.0, wait.UntilEpochMs);
    }

    [Fact]
    public void PlanNext_ChainsThroughMultiplePoisonedRecoverySnapshots_UntilValidSnapshotFound()
    {
        var rows = new[]
        {
            Row(2, receiptState: ReceiptAvailability.PermanentlyFailed),
            Row(3, kind: UpdateKind.Delta, receiptState: ReceiptAvailability.Ready),
            Row(4, kind: UpdateKind.Snapshot, receiptState: ReceiptAvailability.PermanentlyFailed),
            Row(5, kind: UpdateKind.Delta, receiptState: ReceiptAvailability.Ready),
            Row(6, kind: UpdateKind.Snapshot, receiptState: ReceiptAvailability.Ready)
        };

        var plan = _planner.PlanNext(View(rows, lastSeq: 1));
        var apply = Assert.IsType<SyncDecision.ApplyUpdateDecision>(plan.Decision);

        Assert.Equal(6, apply.Row.Seq);
        Assert.Equal(
            new[]
            {
                (2, SkipReason.ReceiptFailed),
                (3, SkipReason.SupersededAfterLoss),
                (4, SkipReason.ReceiptFailed),
                (5, SkipReason.SupersededAfterLoss)
            },
            apply.Skipped.Select(s => (s.Seq, s.Reason)).ToArray());
    }

    [Fact]
    public void PlanNext_MarksStoreBlocked_WhenPoisonedUpdateHasNoSubsequentSnapshot()
    {
        var rows = new[]
        {
            Row(2, receiptState: ReceiptAvailability.PermanentlyFailed),
            Row(3, kind: UpdateKind.Delta, receiptState: ReceiptAvailability.Ready),
            Row(4, kind: UpdateKind.Delta, receiptState: ReceiptAvailability.Ready)
        };

        var plan = _planner.PlanNext(View(rows, lastSeq: 1));
        var blocked = Assert.IsType<SyncDecision.BlockStoreDecision>(plan.Decision);

        Assert.Equal(2, blocked.Seq);
        Assert.Equal(SkipReason.ReceiptFailed, blocked.Reason);
        Assert.Equal(
            new[]
            {
                (2, SkipReason.ReceiptFailed),
                (3, SkipReason.BlockedBehindLostSequence),
                (4, SkipReason.BlockedBehindLostSequence)
            },
            blocked.Skipped.Select(s => (s.Seq, s.Reason)).ToArray());
    }

    [Fact]
    public void PlanNext_WaitsOnSequenceGapUntilTimeout_ThenRecoversViaNextSnapshot()
    {
        var rows = new[]
        {
            Row(3, kind: UpdateKind.Delta, receiptState: ReceiptAvailability.Ready),
            Row(4, kind: UpdateKind.Snapshot, receiptState: ReceiptAvailability.Ready)
        };

        var waiting = _planner.PlanNext(View(
            rows,
            lastSeq: 1,
            nowMs: 10_000.0,
            gap: new SequenceGapTimer(2, 5_000.0),
            gapTimeoutMs: 60_000.0));
        var gapDecision = Assert.IsType<SyncDecision.WaitForMissingSequenceDecision>(waiting.Decision);
        Assert.Equal(2, gapDecision.Seq);

        var expired = _planner.PlanNext(View(
            rows,
            lastSeq: 1,
            nowMs: 70_000.0,
            gap: new SequenceGapTimer(2, 5_000.0),
            gapTimeoutMs: 60_000.0));
        var apply = Assert.IsType<SyncDecision.ApplyUpdateDecision>(expired.Decision);
        Assert.Equal(4, apply.Row.Seq);
        Assert.Equal(
            new[]
            {
                (2, SkipReason.MissingSequence),
                (3, SkipReason.SupersededAfterLoss)
            },
            apply.Skipped.Select(s => (s.Seq, s.Reason)).ToArray());
    }

    [Fact]
    public void PlanNext_BlocksStore_WhenDeltaArrivesAtSeqOneWithoutBaseSnapshot()
    {
        var rows = new[]
        {
            Row(1, kind: UpdateKind.Delta, receiptState: ReceiptAvailability.Ready),
            Row(2, kind: UpdateKind.Delta, receiptState: ReceiptAvailability.Ready)
        };

        var plan = _planner.PlanNext(View(rows, lastSeq: 0));
        var blocked = Assert.IsType<SyncDecision.BlockStoreDecision>(plan.Decision);

        Assert.Equal(1, blocked.Seq);
        Assert.Equal(SkipReason.DeltaWithoutBase, blocked.Reason);
    }

    [Fact]
    public async Task StoreBacklogAdvancer_AdvancesConsecutiveReadyUpdatesAndMarksDrained()
    {
        var fakeRepo = new InMemoryFakeStoreSyncRepository(
        [
            Row(1, kind: UpdateKind.Snapshot),
            Row(2, kind: UpdateKind.Delta),
            Row(3, kind: UpdateKind.Delta)
        ]);
        var advancer = new StoreBacklogAdvancer(
            _planner,
            fakeRepo,
            new NullSyncLogger(),
            new ProcessorOptions());

        var outcome = await advancer.AdvanceStoreAsync(SampleStore, "worker-test-1");

        Assert.Equal(3, outcome.AppliedCount);
        Assert.Equal(0, outcome.SkippedCount);
        Assert.False(outcome.BecameBlocked);
        Assert.True(outcome.StateChanged);
        Assert.Equal(3, fakeRepo.LastSeq);
        Assert.Equal(StoreSyncStatus.Drained, fakeRepo.LastHint?.Status);
    }

    private sealed class NullSyncLogger : ISyncLogger
    {
        public void Debug(string message, object? fields = null) { }
        public void Info(string message, object? fields = null) { }
        public void Warn(string message, object? fields = null) { }
        public void Error(string message, object? fields = null) { }
    }

    private sealed class InMemoryFakeStoreSyncRepository(IReadOnlyList<QueuedUpdate> initialRows) : IStoreSyncRepository
    {
        public int LastSeq { get; private set; }
        public int Version { get; private set; }
        public StoreSchedulingHint? LastHint { get; private set; }

        public Task<StoreStreamSnapshot> LoadStoreViewAsync(StoreIdentity store, double gapTimeoutMs, CancellationToken ct = default)
        {
            var remaining = initialRows.Where(r => r.Seq > LastSeq).ToList();
            return Task.FromResult(new StoreStreamSnapshot(store, LastSeq, remaining, 10_000.0, null, gapTimeoutMs));
        }

        public Task<ApplyOutcome> ApplyUpdateAsync(
            StoreIdentity store,
            QueuedUpdate row,
            int expectedLastSeq,
            IReadOnlyList<SkippedUpdate> skipped,
            string workerId,
            Action? midTransactionHook = null,
            CancellationToken ct = default)
        {
            if (LastSeq != expectedLastSeq)
            {
                return Task.FromResult(ApplyOutcome.Aborted(ApplyAbortReason.CasConflict));
            }

            midTransactionHook?.Invoke();
            LastSeq = row.Seq;
            Version += 1;
            return Task.FromResult(ApplyOutcome.Success(Version));
        }

        public Task RecordSkippedUpdatesAsync(StoreIdentity store, IReadOnlyList<SkippedUpdate> skipped, int? resolvedBySeq, string workerId, CancellationToken ct = default)
            => Task.CompletedTask;

        public Task<string> ComputeStoreFingerprintAsync(StoreIdentity store, CancellationToken ct = default)
            => Task.FromResult("fp");

        public Task UpdateSchedulingHintAsync(StoreIdentity store, string workerId, StoreSchedulingHint hint, CancellationToken ct = default)
        {
            LastHint = hint;
            return Task.CompletedTask;
        }

        public Task<CandidateBatch> FindCandidatesAsync(CancellationToken ct = default)
            => Task.FromResult(new CandidateBatch([], 10_000.0));

        public Task<string> ComputeQueueSignatureAsync(CancellationToken ct = default)
            => Task.FromResult("0:0:0");

        public Task<FleetSyncSummary> GetFleetSummaryAsync(CancellationToken ct = default)
            => Task.FromResult(new FleetSyncSummary(1, Version, 1, 0, 0));
    }
}
