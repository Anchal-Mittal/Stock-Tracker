using System.Text.Json;
using StockStream.Domain.Ports;

namespace StockStream.Application.Logging;

public sealed class ConsoleSyncLogger : ISyncLogger
{
    private readonly string _prefix;
    private readonly int _threshold;

    public ConsoleSyncLogger(string role, string level = "info")
    {
        _prefix = $"{role}:{Environment.ProcessId}";
        _threshold = ParseThreshold(level);
    }

    public void Debug(string message, object? fields = null) => Emit("debug", 10, message, fields);
    public void Info(string message, object? fields = null) => Emit("info", 20, message, fields);
    public void Warn(string message, object? fields = null) => Emit("warn", 30, message, fields);
    public void Error(string message, object? fields = null) => Emit("error", 40, message, fields);

    private void Emit(string levelName, int weight, string message, object? fields)
    {
        if (weight < _threshold) return;

        var ts = DateTimeOffset.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ");
        var extra = fields is not null ? " " + JsonSerializer.Serialize(fields) : string.Empty;
        var line = $"{ts} {levelName.ToUpperInvariant().PadRight(5)} [{_prefix}] {message}{extra}";

        if (weight >= 30)
        {
            Console.Error.WriteLine(line);
        }
        else
        {
            Console.Out.WriteLine(line);
        }
    }

    private static int ParseThreshold(string level) => level.ToLowerInvariant() switch
    {
        "debug" => 10,
        "info" => 20,
        "warn" => 30,
        "error" => 40,
        _ => 20
    };
}
