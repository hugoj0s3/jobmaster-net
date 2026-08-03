using StackExchange.Redis;

namespace JobMaster.Benchmarks.Common.Recording;

public sealed class RedisLatencyRecorder(IConnectionMultiplexer mux, string runId) : ILatencyRecorder
{
    public async Task RecordExpectedAsync(Guid jobId, DateTime expectedAtUtc, bool isImmediate, CancellationToken ct = default)
    {
        var db = mux.GetDatabase();
        var value = $"{expectedAtUtc.Ticks}|{(isImmediate ? 1 : 0)}";
        await db.HashSetAsync($"bench:{runId}:expected", jobId.ToString(), value);
    }
}
