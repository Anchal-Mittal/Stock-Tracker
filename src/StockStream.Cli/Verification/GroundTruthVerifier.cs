using System.Text.Json;
using System.Text.Json.Nodes;
using Npgsql;
using StockStream.Infrastructure.Persistence;

namespace StockStream.Cli.Verification;

public sealed record VerificationReport(
    bool Ok,
    int StoresChecked,
    int TotalQueueRows,
    int DistinctLogicalUpdates,
    int TotalApplied,
    int TotalSkipped,
    IReadOnlyDictionary<string, int> DistinctWorkers,
    IReadOnlyList<string> Violations);

/// <summary>
/// Independent ground-truth verifier. Reads raw queue_messages and receipt_files,
/// replays every store stream from scratch in C#, and checks 7 hard invariants.
/// </summary>
public sealed class GroundTruthVerifier
{
    private readonly string _databaseUrl;

    public GroundTruthVerifier(string databaseUrl)
    {
        _databaseUrl = databaseUrl;
    }

    public async Task<VerificationReport> VerifyAsync(CancellationToken ct = default)
    {
        await using var session = await PostgresSession.ConnectWithRetryAsync(
            _databaseUrl,
            "stockstream-dotnet-verifier",
            ct: ct);
        var conn = session.Connection;
        var violations = new List<string>();

        // 1. Uniqueness of message_id in apply_log
        await using (var dupMsgCmd = new NpgsqlCommand(
            "SELECT message_id::text, count(*) FROM apply_log GROUP BY message_id HAVING count(*) > 1", conn))
        await using (var r = await dupMsgCmd.ExecuteReaderAsync(ct))
        {
            while (await r.ReadAsync(ct))
            {
                violations.Add($"duplicate message_id in apply_log: {r.GetString(0)} (count={r.GetInt64(1)})");
            }
        }

        // 2. Uniqueness of (chain_id, store_id, seq) in apply_log
        await using (var dupSeqCmd = new NpgsqlCommand(
            "SELECT store_id::text, seq, count(*) FROM apply_log GROUP BY chain_id, store_id, seq HAVING count(*) > 1", conn))
        await using (var r = await dupSeqCmd.ExecuteReaderAsync(ct))
        {
            while (await r.ReadAsync(ct))
            {
                violations.Add($"duplicate (chain,store,seq) in apply_log: store={r.GetString(0)} seq={r.GetInt32(1)} (count={r.GetInt64(2)})");
            }
        }

        // 3. Compliance check: no update with NULL ready_at or ready_at > applied_at was ever applied
        await using (var compCmd = new NpgsqlCommand(
            """
            SELECT al.message_id::text, al.store_id::text, al.seq,
                   rf.ready_at::text, al.applied_at::text
              FROM apply_log al
              JOIN LATERAL (
                SELECT qm.receipt_id
                  FROM queue_messages qm
                 WHERE qm.message_id = al.message_id
                 ORDER BY qm.id ASC
                 LIMIT 1
              ) q ON true
              JOIN receipt_files rf ON rf.id = q.receipt_id
             WHERE rf.ready_at IS NULL OR rf.ready_at > al.applied_at
            """, conn))
        await using (var r = await compCmd.ExecuteReaderAsync(ct))
        {
            while (await r.ReadAsync(ct))
            {
                var readyAtStr = r.IsDBNull(3) ? "NULL" : r.GetString(3);
                violations.Add(
                    $"compliance violation on store={r.GetString(1)} seq={r.GetInt32(2)} msg={r.GetString(0)}: ready_at={readyAtStr} applied_at={r.GetString(4)}");
            }
        }

        // 4. Per-store strict monotonicity of seq ordered by apply_pos
        await using (var monoCmd = new NpgsqlCommand(
            """
            WITH ordered AS (
              SELECT store_id::text AS store_id, seq,
                     lag(seq) OVER (PARTITION BY chain_id, store_id ORDER BY apply_pos ASC) AS prev_seq
                FROM apply_log
            )
            SELECT store_id, prev_seq, seq
              FROM ordered
             WHERE prev_seq IS NOT NULL AND seq <= prev_seq
            """, conn))
        await using (var r = await monoCmd.ExecuteReaderAsync(ct))
        {
            while (await r.ReadAsync(ct))
            {
                violations.Add($"non-monotonic apply_log order for store={r.GetString(0)}: {r.GetInt32(1)} -> {r.GetInt32(2)}");
            }
        }

        // 5. Load raw queue rows, store_stock, apply_log, and skipped_updates for independent reference replay
        var rawRows = new List<RawQueueItem>();
        await using (var qCmd = new NpgsqlCommand(
            """
            SELECT q.id, q.message_id::text, q.chain_id::text, q.store_id::text,
                   q.seq, q.kind, q.payload::text,
                   (extract(epoch FROM r.ready_at) * 1000)::float8 AS ready_at_ms
              FROM queue_messages q
              JOIN receipt_files r ON r.id = q.receipt_id
             ORDER BY q.id ASC
            """, conn))
        await using (var r = await qCmd.ExecuteReaderAsync(ct))
        {
            while (await r.ReadAsync(ct))
            {
                rawRows.Add(new RawQueueItem(
                    Id: r.GetInt64(0),
                    MessageId: r.GetString(1),
                    ChainId: r.GetString(2),
                    StoreId: r.GetString(3),
                    Seq: r.GetInt32(4),
                    Kind: r.GetString(5),
                    PayloadJson: r.GetString(6),
                    ReadyAtMs: r.IsDBNull(7) ? null : r.GetDouble(7)));
            }
        }

        var actualStocks = new Dictionary<string, ActualStockRow>();
        await using (var sCmd = new NpgsqlCommand(
            "SELECT chain_id::text, store_id::text, stock::text, version, last_seq FROM store_stock", conn))
        await using (var r = await sCmd.ExecuteReaderAsync(ct))
        {
            while (await r.ReadAsync(ct))
            {
                var key = $"{r.GetString(0)}/{r.GetString(1)}";
                actualStocks[key] = new ActualStockRow(r.GetString(2), r.GetInt32(3), r.GetInt32(4));
            }
        }

        var actualLogsByStore = new Dictionary<string, List<ActualLogRow>>();
        var distinctWorkers = new Dictionary<string, int>();
        var totalApplied = 0;

        await using (var lCmd = new NpgsqlCommand(
            """
            SELECT al.apply_pos, al.message_id::text, al.chain_id::text, al.store_id::text,
                   al.seq, al.kind, al.worker_id
              FROM apply_log al
             ORDER BY al.apply_pos ASC
            """, conn))
        await using (var r = await lCmd.ExecuteReaderAsync(ct))
        {
            while (await r.ReadAsync(ct))
            {
                totalApplied++;
                var applyPos = r.GetInt64(0);
                var msgId = r.GetString(1);
                var key = $"{r.GetString(2)}/{r.GetString(3)}";
                var seq = r.GetInt32(4);
                var kind = r.GetString(5);
                var workerId = r.GetString(6);

                if (string.IsNullOrWhiteSpace(workerId))
                {
                    violations.Add($"empty worker_id in apply_log at apply_pos={applyPos}");
                }

                distinctWorkers[workerId] = distinctWorkers.GetValueOrDefault(workerId) + 1;

                if (!actualLogsByStore.TryGetValue(key, out var list))
                {
                    list = [];
                    actualLogsByStore[key] = list;
                }
                list.Add(new ActualLogRow(seq, msgId, kind));
            }
        }

        var actualSkippedByStore = new Dictionary<string, List<int>>();
        var totalSkipped = 0;
        await using (var skCmd = new NpgsqlCommand(
            "SELECT chain_id::text, store_id::text, seq FROM skipped_updates ORDER BY seq ASC", conn))
        await using (var r = await skCmd.ExecuteReaderAsync(ct))
        {
            while (await r.ReadAsync(ct))
            {
                totalSkipped++;
                var key = $"{r.GetString(0)}/{r.GetString(1)}";
                if (!actualSkippedByStore.TryGetValue(key, out var list))
                {
                    list = [];
                    actualSkippedByStore[key] = list;
                }
                list.Add(r.GetInt32(2));
            }
        }

        var byStore = rawRows.GroupBy(x => $"{x.ChainId}/{x.StoreId}").ToList();
        var distinctLogicalUpdates = 0;

        foreach (var storeGroup in byStore)
        {
            var key = storeGroup.Key;
            var bySeq = new SortedDictionary<int, RawQueueItem>();
            foreach (var row in storeGroup)
            {
                // Earliest arrival id wins (rawRows is ordered by q.id ASC)
                if (!bySeq.ContainsKey(row.Seq))
                {
                    bySeq[row.Seq] = row;
                }
            }

            distinctLogicalUpdates += bySeq.Count;
            var seqs = bySeq.Keys.ToArray();
            var maxSeq = seqs.Length > 0 ? seqs[^1] : 0;

            var expectedStock = new SortedDictionary<string, string>(StringComparer.Ordinal);
            var expectedVersion = 0;
            var expectedLastSeq = 0;
            var expectedApplied = new List<ActualLogRow>();
            var expectedSkipped = new List<int>();

            var cursor = 1;
            while (cursor <= maxSeq)
            {
                bySeq.TryGetValue(cursor, out var msg);
                var isLost = msg is null || msg.ReadyAtMs is null || (msg.Kind == "delta" && expectedVersion == 0);

                if (!isLost && msg is not null)
                {
                    ApplyPayloadToExpectedStock(expectedStock, msg.Kind, msg.PayloadJson);
                    expectedVersion += 1;
                    expectedLastSeq = cursor;
                    expectedApplied.Add(new ActualLogRow(cursor, msg.MessageId, msg.Kind));
                    cursor += 1;
                    continue;
                }

                expectedSkipped.Add(cursor);
                int? nextSnapSeq = null;
                foreach (var s in seqs)
                {
                    if (s > cursor && bySeq[s].Kind == "snapshot")
                    {
                        nextSnapSeq = s;
                        break;
                    }
                }

                var limit = nextSnapSeq ?? (maxSeq + 1);
                foreach (var s in seqs)
                {
                    if (s > cursor && s < limit)
                    {
                        expectedSkipped.Add(s);
                    }
                }

                if (nextSnapSeq is null) break;
                cursor = nextSnapSeq.Value;
            }

            var hasActualStock = actualStocks.TryGetValue(key, out var actualStock);
            var actualLogs = actualLogsByStore.GetValueOrDefault(key) ?? [];
            var actualSkipped = actualSkippedByStore.GetValueOrDefault(key) ?? [];

            if (expectedVersion == 0)
            {
                if (hasActualStock)
                {
                    violations.Add($"store {key} should have no store_stock row, but found one");
                }
            }
            else if (!hasActualStock || actualStock is null)
            {
                violations.Add($"store {key} missing from store_stock (expected version={expectedVersion}, last_seq={expectedLastSeq})");
            }
            else
            {
                if (actualStock.Version != expectedVersion)
                {
                    violations.Add($"store {key} version mismatch: actual={actualStock.Version} expected={expectedVersion}");
                }
                if (actualStock.LastSeq != expectedLastSeq)
                {
                    violations.Add($"store {key} last_seq mismatch: actual={actualStock.LastSeq} expected={expectedLastSeq}");
                }

                var actualCanonicalStock = CanonicalizeStockJson(actualStock.StockJson);
                var expectedCanonicalStock = CanonicalizeStockMap(expectedStock);
                if (actualCanonicalStock != expectedCanonicalStock)
                {
                    violations.Add(
                        $"store {key} stock mismatch:\n  actual:   {actualCanonicalStock}\n  expected: {expectedCanonicalStock}");
                }
            }

            if (actualLogs.Count != expectedApplied.Count)
            {
                violations.Add($"store {key} apply_log count mismatch: actual={actualLogs.Count} expected={expectedApplied.Count}");
            }
            else
            {
                for (var i = 0; i < expectedApplied.Count; i++)
                {
                    if (actualLogs[i] != expectedApplied[i])
                    {
                        violations.Add(
                            $"store {key} apply_log[{i}] mismatch: actual={actualLogs[i]} expected={expectedApplied[i]}");
                    }
                }
            }

            if (!actualSkipped.SequenceEqual(expectedSkipped))
            {
                violations.Add(
                    $"store {key} skipped_updates mismatch: actual=[{string.Join(",", actualSkipped)}] expected=[{string.Join(",", expectedSkipped)}]");
            }
        }

        return new VerificationReport(
            Ok: violations.Count == 0,
            StoresChecked: byStore.Count,
            TotalQueueRows: rawRows.Count,
            DistinctLogicalUpdates: distinctLogicalUpdates,
            TotalApplied: totalApplied,
            TotalSkipped: totalSkipped,
            DistinctWorkers: distinctWorkers,
            Violations: violations);
    }

    private static void ApplyPayloadToExpectedStock(
        SortedDictionary<string, string> stock,
        string kind,
        string payloadJson)
    {
        var node = JsonNode.Parse(payloadJson)!.AsObject();
        if (kind == "snapshot")
        {
            stock.Clear();
            var stockObj = node["stock"]!.AsObject();
            foreach (var (k, v) in stockObj)
            {
                stock[k] = NormalizeScalar(v);
            }
        }
        else
        {
            if (node["set"] is JsonObject setObj)
            {
                foreach (var (k, v) in setObj)
                {
                    stock[k] = NormalizeScalar(v);
                }
            }
            if (node["unset"] is JsonArray unsetArr)
            {
                foreach (var item in unsetArr)
                {
                    if (item is not null)
                    {
                        stock.Remove(item.GetValue<string>());
                    }
                }
            }
        }
    }

    private static string CanonicalizeStockJson(string json)
    {
        var map = new SortedDictionary<string, string>(StringComparer.Ordinal);
        var obj = JsonNode.Parse(json)!.AsObject();
        foreach (var (k, v) in obj)
        {
            map[k] = NormalizeScalar(v);
        }
        return CanonicalizeStockMap(map);
    }

    private static string CanonicalizeStockMap(SortedDictionary<string, string> map)
    {
        return "{" + string.Join(",", map.Select(kv => $"{JsonSerializer.Serialize(kv.Key)}:{kv.Value}")) + "}";
    }

    private static string NormalizeScalar(JsonNode? node)
    {
        if (node is null) return "null";
        if (node is JsonValue val)
        {
            if (val.TryGetValue<string>(out var s)) return JsonSerializer.Serialize(s);
            if (val.TryGetValue<double>(out var d)) return d.ToString("G17", System.Globalization.CultureInfo.InvariantCulture);
            if (val.TryGetValue<bool>(out var b)) return b ? "true" : "false";
        }
        return node.ToJsonString();
    }

    private sealed record RawQueueItem(
        long Id,
        string MessageId,
        string ChainId,
        string StoreId,
        int Seq,
        string Kind,
        string PayloadJson,
        double? ReadyAtMs);

    private sealed record ActualStockRow(string StockJson, int Version, int LastSeq);
    private sealed record ActualLogRow(int Seq, string MessageId, string Kind);
}
