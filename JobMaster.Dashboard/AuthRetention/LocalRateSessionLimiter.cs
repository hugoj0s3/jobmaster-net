namespace JobMaster.Dashboard.AuthRetention;

internal sealed class LocalRateSessionLimiter : IRateSessionLimiter
{
    private sealed class SessionEntry
    {
        public string SessionId { get; set; } = string.Empty;
        public int Count { get; set; }
        public DateTime WindowStart { get; set; }
    }

    private readonly Dictionary<string, SessionEntry> entries = new Dictionary<string, SessionEntry>();
    private readonly object syncLock = new object();
    private readonly int max;
    private readonly TimeSpan window;

    public LocalRateSessionLimiter(int max, TimeSpan window)
    {
        this.max = max;
        this.window = window;
    }

    public Task<RateSessionResult> TryOpenSessionAsync(string clientId, string newSessionId, CancellationToken ct = default)
    {
        var now = DateTime.UtcNow;

        lock (syncLock)
        {
            if (!entries.TryGetValue(clientId, out var entry))
            {
                entries[clientId] = new SessionEntry { SessionId = newSessionId, Count = 1, WindowStart = now };
                return Task.FromResult(RateSessionResult.Granted(null));
            }

            if (now - entry.WindowStart > window)
            {
                var prev = entry.SessionId;
                entries[clientId] = new SessionEntry { SessionId = newSessionId, Count = 1, WindowStart = now };
                return Task.FromResult(RateSessionResult.Granted(prev));
            }

            if (entry.Count >= max)
                return Task.FromResult(RateSessionResult.Denied);

            var previous = entry.SessionId;
            entry.SessionId = newSessionId;
            entry.Count++;
            return Task.FromResult(RateSessionResult.Granted(previous));
        }
    }
}
