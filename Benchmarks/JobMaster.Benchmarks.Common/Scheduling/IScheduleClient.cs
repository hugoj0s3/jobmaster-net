namespace JobMaster.Benchmarks.Common.Scheduling;

/// <summary>
/// Framework-agnostic scheduling contract every benchmarked framework's HTTP host satisfies
/// identically (JobMaster, Quartz, Hangfire, TickerQ each expose the same two endpoints) -- this is
/// what makes the load generator, latency measurement, and reporting pipeline reusable across all
/// four frameworks without any framework-specific branching.
/// </summary>
public interface IScheduleClient
{
    Task<IReadOnlyList<Guid>> ScheduleNowAsync(int count, CancellationToken ct = default);

    Task<IReadOnlyList<Guid>> ScheduleAfterAsync(int count, TimeSpan delay, CancellationToken ct = default);
}
