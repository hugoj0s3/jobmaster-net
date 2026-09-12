using StackExchange.Redis;

namespace JobMaster.Benchmarks.Common.Recording;

public sealed class RedisCompletionRecorder(IConnectionMultiplexer mux, string runId) : ICompletionRecorder
{
    public async Task RecordCompletionAsync(Guid jobId, CancellationToken ct = default)
    {
        var db = mux.GetDatabase();
        var value = $"{jobId}|{DateTime.UtcNow.Ticks}";
        await db.ListRightPushAsync($"bench:{runId}:completions", value);
    }
}
