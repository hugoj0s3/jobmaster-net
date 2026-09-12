namespace JobMaster.Benchmarks.Common.Scheduling;

/// <summary>
/// Spreads schedule calls round-robin across every worker container's own HTTP endpoint --
/// JobMaster's scheduling API is a client-library call handled by whichever process's DI container
/// it runs in, not something that must go through a coordinator specifically, so any container
/// registered against the same cluster/database can accept a schedule request. Simulates "any node
/// can accept the request" without needing a full reverse-proxy layer.
/// </summary>
public sealed class RoundRobinScheduleClient(IReadOnlyList<IScheduleClient> clients) : IScheduleClient
{
    private int index = -1;

    public Task<IReadOnlyList<Guid>> ScheduleNowAsync(int count, CancellationToken ct = default) =>
        Next().ScheduleNowAsync(count, ct);

    public Task<IReadOnlyList<Guid>> ScheduleAfterAsync(int count, TimeSpan delay, CancellationToken ct = default) =>
        Next().ScheduleAfterAsync(count, delay, ct);

    private IScheduleClient Next()
    {
        var i = Interlocked.Increment(ref index);
        return clients[(int)((uint)i % clients.Count)];
    }
}
