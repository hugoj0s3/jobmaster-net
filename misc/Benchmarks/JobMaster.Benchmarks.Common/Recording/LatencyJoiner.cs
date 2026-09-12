using StackExchange.Redis;

namespace JobMaster.Benchmarks.Common.Recording;

/// <summary>
/// Post-run analysis: joins the expected-time hash (written by <see cref="RedisLatencyRecorder"/>,
/// which also serves as the full scheduled-job-ID set) against the completions list (written by
/// <see cref="RedisCompletionRecorder"/>) to compute latency percentiles plus lost/duplicated job
/// counts. Call this only after a grace period past the run's last expected completion time, so
/// jobs still legitimately in flight aren't misreported as lost.
///
/// Loads the full run's data into memory in one pass -- fine at the scale Phase A runs at (tens of
/// thousands of jobs); the largest planned tiers (100k jobs/min x 60 min = ~6M jobs) will likely
/// need a streaming/batched rewrite before then.
/// </summary>
public sealed class LatencyJoiner(IConnectionMultiplexer mux, string runId)
{
    public async Task<LatencyReport> ComputeAsync(CancellationToken ct = default)
    {
        var db = mux.GetDatabase();

        var expectedEntries = await db.HashGetAllAsync($"bench:{runId}:expected");
        var expected = new Dictionary<Guid, (DateTime ExpectedAtUtc, bool IsImmediate)>(expectedEntries.Length);
        foreach (var entry in expectedEntries)
        {
            var jobId = Guid.Parse(entry.Name!);
            var parts = ((string)entry.Value!).Split('|');
            expected[jobId] = (new DateTime(long.Parse(parts[0]), DateTimeKind.Utc), parts[1] == "1");
        }

        var completionEntries = await db.ListRangeAsync($"bench:{runId}:completions");
        var completionsByJob = new Dictionary<Guid, List<DateTime>>();
        DateTime? firstCompletionAtUtc = null;
        DateTime? lastCompletionAtUtc = null;
        foreach (var entry in completionEntries)
        {
            var parts = ((string)entry!).Split('|');
            var jobId = Guid.Parse(parts[0]);
            var actualAtUtc = new DateTime(long.Parse(parts[1]), DateTimeKind.Utc);
            if (!completionsByJob.TryGetValue(jobId, out var list))
                completionsByJob[jobId] = list = [];
            list.Add(actualAtUtc);

            if (firstCompletionAtUtc is null || actualAtUtc < firstCompletionAtUtc) firstCompletionAtUtc = actualAtUtc;
            if (lastCompletionAtUtc is null || actualAtUtc > lastCompletionAtUtc) lastCompletionAtUtc = actualAtUtc;
        }

        var lost = new List<Guid>();
        var duplicated = new List<Guid>();
        var immediateLatenciesMs = new List<double>();
        var delayedLatenciesMs = new List<double>();

        foreach (var (jobId, (expectedAtUtc, isImmediate)) in expected)
        {
            if (!completionsByJob.TryGetValue(jobId, out var actuals) || actuals.Count == 0)
            {
                lost.Add(jobId);
                continue;
            }

            if (actuals.Count > 1)
            {
                duplicated.Add(jobId);
            }

            var firstCompletion = actuals.Min();
            var latencyMs = (firstCompletion - expectedAtUtc).TotalMilliseconds;
            (isImmediate ? immediateLatenciesMs : delayedLatenciesMs).Add(latencyMs);
        }

        return new LatencyReport(
            TotalScheduled: expected.Count,
            TotalCompletedJobs: completionsByJob.Count,
            LostCount: lost.Count,
            DuplicatedCount: duplicated.Count,
            LostJobIds: lost,
            DuplicatedJobIds: duplicated,
            Immediate: LatencyPercentiles.FromMilliseconds(immediateLatenciesMs),
            Delayed: LatencyPercentiles.FromMilliseconds(delayedLatenciesMs),
            FirstCompletionAtUtc: firstCompletionAtUtc,
            LastCompletionAtUtc: lastCompletionAtUtc);
    }
}
