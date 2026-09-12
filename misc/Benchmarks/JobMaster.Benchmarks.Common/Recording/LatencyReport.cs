namespace JobMaster.Benchmarks.Common.Recording;

public sealed record LatencyPercentiles(double MeanMs, double P50Ms, double P90Ms, double P99Ms, double MaxMs, int SampleCount)
{
    public static LatencyPercentiles FromMilliseconds(IReadOnlyCollection<double> valuesMs)
    {
        if (valuesMs.Count == 0)
        {
            return new LatencyPercentiles(0, 0, 0, 0, 0, 0);
        }

        var sorted = valuesMs.OrderBy(v => v).ToList();
        return new LatencyPercentiles(
            MeanMs: sorted.Average(),
            P50Ms: Percentile(sorted, 0.50),
            P90Ms: Percentile(sorted, 0.90),
            P99Ms: Percentile(sorted, 0.99),
            MaxMs: sorted[^1],
            SampleCount: sorted.Count);
    }

    private static double Percentile(List<double> sortedValues, double p)
    {
        var index = (int)Math.Ceiling(p * sortedValues.Count) - 1;
        return sortedValues[Math.Clamp(index, 0, sortedValues.Count - 1)];
    }
}

/// <summary>Client-perceived time for a schedule HTTP call itself to return -- separate from
/// <see cref="LatencyReport"/>'s end-to-end due-time-to-execution latency. Measures how fast the
/// framework under test accepts new schedule requests, independent of whether it can keep up with
/// dispatching its backlog.</summary>
public sealed record ScheduleCallLatencyReport(LatencyPercentiles Immediate, LatencyPercentiles Delayed);

public sealed record LatencyReport(
    int TotalScheduled,
    int TotalCompletedJobs,
    int LostCount,
    int DuplicatedCount,
    IReadOnlyList<Guid> LostJobIds,
    IReadOnlyList<Guid> DuplicatedJobIds,
    LatencyPercentiles Immediate,
    LatencyPercentiles Delayed,
    // The actual wall-clock span completions landed in -- null when TotalCompletedJobs is 0.
    // TotalCompletedJobs / (LastCompletionAtUtc - FirstCompletionAtUtc) is the real sustained
    // completion throughput; dividing by the load-generation window's nominal duration instead
    // (as an earlier version of this report did) is wrong whenever delayed jobs or execution
    // latency push completions past the load window -- which they always do to some degree.
    DateTime? FirstCompletionAtUtc,
    DateTime? LastCompletionAtUtc);
