using System.Text.Json;
using Microsoft.Extensions.Caching.Distributed;

namespace JobMaster.Dashboard.Credentials;

/// <summary>
/// Fixed-window rate limiter backed by <see cref="IDistributedCache"/>.
/// Uses a read-check-write pattern which is not atomic — concurrent requests from the same
/// client may occasionally exceed the limit by a small margin. This is acceptable for
/// abuse prevention where exact enforcement is not required.
/// </summary>
internal sealed class DistributedRateSessionLimiter : IRateSessionLimiter
{
    private sealed class SessionEntry
    {
        public string SessionId { get; set; } = string.Empty;
        public int Count { get; set; }
        public DateTime WindowStart { get; set; }
    }

    private readonly IDistributedCache cache;
    private readonly int max;
    private readonly TimeSpan window;
    private readonly Random random = new Random();

    public DistributedRateSessionLimiter(IDistributedCache cache, int max, TimeSpan window)
    {
        this.cache = cache;
        this.max = max;
        this.window = window;
    }

    public async Task<RateSessionResult> TryOpenSessionAsync(string clientId, string newSessionId, CancellationToken ct = default)
    {
        var now = DateTime.UtcNow;
        var key = $"jm:rsl:{clientId}";

        var entry = await GetSessionEntry(newSessionId, ct, key, now);

        SessionEntry next;
        string? previousSessionId;
        
        if (entry.Count >= max)
        {
            return RateSessionResult.Denied;
        }
        
        var delayTicks = entry.Count == 0
            ? random.Next(1, 10) * (window.Ticks / 1000)
            : entry.Count * (window.Ticks / 100);
        await Task.Delay(TimeSpan.FromTicks(delayTicks), ct);
        
        entry = await GetSessionEntry(newSessionId, ct, key, now);
        
        previousSessionId = entry.SessionId;
        next = new SessionEntry { SessionId = newSessionId, Count = entry.Count + 1, WindowStart = entry.WindowStart };
        
        var cacheOptions = new DistributedCacheEntryOptions
        {
            AbsoluteExpirationRelativeToNow = window
        };
        await cache.SetStringAsync(key, JsonSerializer.Serialize(next), cacheOptions, ct);

        return RateSessionResult.Granted(previousSessionId);
    }

    private async Task<SessionEntry> GetSessionEntry(string newSessionId, CancellationToken ct, string key, DateTime now)
    {
        var json = await cache.GetStringAsync(key, ct);
        var entry = json is null ? 
            new SessionEntry() { SessionId = newSessionId, Count = 0, WindowStart = now } : 
            JsonSerializer.Deserialize<SessionEntry>(json)!;

        return entry;
    }
}
