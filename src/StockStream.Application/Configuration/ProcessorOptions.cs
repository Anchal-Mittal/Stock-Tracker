namespace StockStream.Application.Configuration;

public sealed record ProcessorOptions
{
    public string DatabaseUrl { get; init; } = "postgres://postgres:postgres@localhost:5439/stockstream";
    public int WorkerCount { get; init; } = 4;
    public int MaxIdleSleepMs { get; init; } = 400;
    public int MinIdleSleepMs { get; init; } = 50;
    public int DrainQuietMs { get; init; } = 800;
    public int GapTimeoutMs { get; init; } = 60_000;
    public int MaxRestartsPerSlot { get; init; } = 25;
    public int RestartBackoffMs { get; init; } = 250;
    public double ChaosCrashProb { get; init; } = 0.0;
    public int MaxAppliesPerClaim { get; init; } = 32;
    public string LogLevel { get; init; } = "info";

    public static ProcessorOptions FromEnvironment() => new()
    {
        DatabaseUrl = GetString("DATABASE_URL", "postgres://postgres:postgres@localhost:5439/stockstream"),
        WorkerCount = GetInt("WORKER_COUNT", 4),
        MaxIdleSleepMs = GetInt("MAX_IDLE_SLEEP_MS", 400),
        MinIdleSleepMs = GetInt("MIN_IDLE_SLEEP_MS", 50),
        DrainQuietMs = GetInt("DRAIN_QUIET_MS", 800),
        GapTimeoutMs = GetInt("GAP_TIMEOUT_MS", 60_000),
        MaxRestartsPerSlot = GetInt("MAX_RESTARTS_PER_SLOT", 25),
        RestartBackoffMs = GetInt("RESTART_BACKOFF_MS", 250),
        ChaosCrashProb = GetDouble("CHAOS_CRASH_PROB", 0.0),
        MaxAppliesPerClaim = GetInt("MAX_APPLIES_PER_CLAIM", 32),
        LogLevel = GetString("LOG_LEVEL", "info").ToLowerInvariant()
    };

    private static string GetString(string name, string fallback)
    {
        var raw = Environment.GetEnvironmentVariable(name);
        return string.IsNullOrWhiteSpace(raw) ? fallback : raw.Trim();
    }

    private static int GetInt(string name, int fallback)
    {
        var raw = Environment.GetEnvironmentVariable(name);
        if (string.IsNullOrWhiteSpace(raw)) return fallback;
        return int.TryParse(raw.Trim(), out var v)
            ? v
            : throw new InvalidOperationException($"Environment variable {name} must be an integer, got '{raw}'.");
    }

    private static double GetDouble(string name, double fallback)
    {
        var raw = Environment.GetEnvironmentVariable(name);
        if (string.IsNullOrWhiteSpace(raw)) return fallback;
        return double.TryParse(raw.Trim(), System.Globalization.CultureInfo.InvariantCulture, out var v)
            ? v
            : throw new InvalidOperationException($"Environment variable {name} must be a number, got '{raw}'.");
    }
}
