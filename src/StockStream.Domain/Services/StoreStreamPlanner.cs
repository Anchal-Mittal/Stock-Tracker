using StockStream.Domain.Decisions;
using StockStream.Domain.Entities;
using StockStream.Domain.ValueObjects;

namespace StockStream.Domain.Services;

/// <summary>
/// Pure, zero-I/O Domain Service implementing StockStream's per-store causal ordering,
/// duplicate collapsing, receipt readiness barrier, and skip-to-snapshot poison recovery.
/// </summary>
public sealed class StoreStreamPlanner : IStoreStreamPlanner
{
    public sealed record SequenceSlot(
        int Seq,
        QueuedUpdate CanonicalRow,
        ReceiptAvailability EffectiveReceiptState,
        double? EffectiveReadyAtEpochMs);

    public StorePlanResult PlanNext(StoreStreamSnapshot view)
    {
        ArgumentNullException.ThrowIfNull(view);

        var (slots, anomalies) = CollapseDuplicates(view.OutstandingRows.Where(r => r.Seq > view.LastSeq));
        var seqs = slots.Keys.OrderBy(s => s).ToArray();
        var maxSeq = seqs.Length > 0 ? seqs[^1] : view.LastSeq;

        var skipped = new List<SkippedUpdate>();
        var cursor = view.LastSeq + 1;
        var recovering = false;

        while (true)
        {
            if (!slots.TryGetValue(cursor, out var slot))
            {
                if (cursor > maxSeq)
                {
                    if (!recovering)
                    {
                        return new StorePlanResult(SyncDecision.StoreDrainedDecision.Instance, anomalies);
                    }

                    return BuildBlockedPlan(skipped, anomalies);
                }

                // Sequence gap: seq == cursor is missing while seq > cursor exists.
                SequenceGapTimer? activeTimer = view.ActiveGap is { } g && g.Seq == cursor ? g : null;
                var expired = activeTimer is { } t && (view.NowEpochMs - t.SinceEpochMs) >= view.GapTimeoutMs;

                if (!expired)
                {
                    if (recovering)
                    {
                        throw new InvalidOperationException(
                            $"Planner invariant violated: missing recovery snapshot at seq={cursor}.");
                    }

                    return new StorePlanResult(new SyncDecision.WaitForMissingSequenceDecision(cursor), anomalies);
                }

                var nextRecoveryAfterGap = LoseAndRecover(cursor, null, SkipReason.MissingSequence, seqs, maxSeq, slots, skipped);
                if (nextRecoveryAfterGap is null)
                {
                    return BuildBlockedPlan(skipped, anomalies);
                }

                cursor = nextRecoveryAfterGap.Value;
                recovering = true;
                continue;
            }

            var row = slot.CanonicalRow;
            SkipReason? lossReason = null;

            if (!row.PayloadValid)
            {
                lossReason = SkipReason.InvalidPayload;
            }
            else if (slot.EffectiveReceiptState == ReceiptAvailability.PermanentlyFailed)
            {
                lossReason = SkipReason.ReceiptFailed;
            }
            else if (row.Kind == UpdateKind.Delta && cursor == view.LastSeq + 1 && view.LastSeq == 0)
            {
                lossReason = SkipReason.DeltaWithoutBase;
            }

            if (lossReason is null && slot.EffectiveReceiptState == ReceiptAvailability.Pending)
            {
                // Strict per-store ordering barrier: later updates must wait for this receipt.
                var untilMs = slot.EffectiveReadyAtEpochMs ?? (view.NowEpochMs + 1000.0);
                return new StorePlanResult(new SyncDecision.WaitForReceiptDecision(cursor, untilMs), anomalies);
            }

            if (lossReason is null)
            {
                return new StorePlanResult(
                    new SyncDecision.ApplyUpdateDecision(row, view.LastSeq, skipped),
                    anomalies);
            }

            var nextRecoverySeq = LoseAndRecover(cursor, row, lossReason.Value, seqs, maxSeq, slots, skipped);
            if (nextRecoverySeq is null)
            {
                return BuildBlockedPlan(skipped, anomalies);
            }

            cursor = nextRecoverySeq.Value;
            recovering = true;
        }
    }

    public static (Dictionary<int, SequenceSlot> Slots, List<StreamAnomaly> Anomalies) CollapseDuplicates(
        IEnumerable<QueuedUpdate> rows)
    {
        var bySeq = new Dictionary<int, List<QueuedUpdate>>();
        foreach (var r in rows)
        {
            if (!bySeq.TryGetValue(r.Seq, out var list))
            {
                list = [];
                bySeq[r.Seq] = list;
            }
            list.Add(r);
        }

        var slots = new Dictionary<int, SequenceSlot>(bySeq.Count);
        var anomalies = new List<StreamAnomaly>();

        foreach (var (seq, list) in bySeq)
        {
            list.Sort((a, b) => a.QueueRowId.CompareTo(b.QueueRowId));
            var canonical = list[0];
            var copies = list.Where(r => r.MessageId == canonical.MessageId).ToList();

            if (copies.Count != list.Count)
            {
                var conflictingIds = list
                    .Where(r => r.MessageId != canonical.MessageId)
                    .Select(r => r.MessageId.ToString("D"))
                    .Distinct();
                anomalies.Add(new StreamAnomaly(
                    seq,
                    $"conflicting message_ids for seq={seq}; using {canonical.MessageId:D}, ignoring {string.Join(",", conflictingIds)}"));
            }

            var effectiveState = ReceiptAvailability.PermanentlyFailed;
            double? effectiveReadyAtMs = null;

            foreach (var copy in copies)
            {
                if (copy.ReceiptState == ReceiptAvailability.Ready)
                {
                    effectiveState = ReceiptAvailability.Ready;
                    effectiveReadyAtMs = copy.ReadyAtEpochMs;
                    break;
                }

                if (copy.ReceiptState == ReceiptAvailability.Pending)
                {
                    effectiveState = ReceiptAvailability.Pending;
                    if (copy.ReadyAtEpochMs is { } ms)
                    {
                        effectiveReadyAtMs = effectiveReadyAtMs is null ? ms : Math.Min(effectiveReadyAtMs.Value, ms);
                    }
                }
            }

            var usableRow = copies.FirstOrDefault(c => c.ReceiptState == effectiveState) ?? canonical;
            slots[seq] = new SequenceSlot(seq, usableRow, effectiveState, effectiveReadyAtMs);
        }

        return (slots, anomalies);
    }

    private static int? LoseAndRecover(
        int lostSeq,
        QueuedUpdate? lostRow,
        SkipReason reason,
        int[] sortedSeqs,
        int maxSeq,
        Dictionary<int, SequenceSlot> slots,
        List<SkippedUpdate> skipped)
    {
        skipped.Add(new SkippedUpdate(lostSeq, lostRow, reason));

        int? recoverySnapshotSeq = null;
        foreach (var s in sortedSeqs)
        {
            if (s > lostSeq && slots[s].CanonicalRow.Kind == UpdateKind.Snapshot)
            {
                recoverySnapshotSeq = s;
                break;
            }
        }

        var untilSeq = recoverySnapshotSeq ?? (maxSeq + 1);
        var dependentReason = recoverySnapshotSeq is not null
            ? SkipReason.SupersededAfterLoss
            : SkipReason.BlockedBehindLostSequence;

        foreach (var s in sortedSeqs)
        {
            if (s > lostSeq && s < untilSeq)
            {
                skipped.Add(new SkippedUpdate(s, slots[s].CanonicalRow, dependentReason));
            }
        }

        return recoverySnapshotSeq;
    }

    private static StorePlanResult BuildBlockedPlan(
        List<SkippedUpdate> skipped,
        List<StreamAnomaly> anomalies)
    {
        var lastRootLoss = skipped.LastOrDefault(s =>
            s.Reason != SkipReason.SupersededAfterLoss &&
            s.Reason != SkipReason.BlockedBehindLostSequence) ?? skipped[0];

        return new StorePlanResult(
            new SyncDecision.BlockStoreDecision(lastRootLoss.Seq, lastRootLoss.Reason, skipped),
            anomalies);
    }
}
