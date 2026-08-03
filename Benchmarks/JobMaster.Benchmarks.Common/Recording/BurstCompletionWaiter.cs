using StackExchange.Redis;

namespace JobMaster.Benchmarks.Common.Recording;

public sealed record CompletionSample(TimeSpan Elapsed, int CompletedCount);

/// <summary>
/// Burst-mode has no fixed grace period like the paced test used to (there's no DelayMax to derive
/// one from -- every job is immediate) -- instead this polls the raw completions list until it
/// reaches the expected count or <paramref name="maxWait"/> elapses, whichever comes first. On
/// timeout, whatever didn't complete shows up as "lost" once <see cref="LatencyJoiner"/> runs, which
/// is the correct outcome for a capacity test: it means the framework couldn't drain the burst in
/// the allotted time. The paced test's post-load-generation wait now uses this same helper too, so
/// every run (paced or burst) gets a completion-count-over-time curve, not just a single elapsed
/// number -- shows ramp-up/steady-state/drain-off rather than just "how long, total."
/// </summary>
public static class BurstCompletionWaiter
{
    public static async Task<(TimeSpan Elapsed, IReadOnlyList<CompletionSample> Timeline)> WaitAsync(
        IConnectionMultiplexer mux,
        string runId,
        int totalExpected,
        TimeSpan maxWait,
        TimeSpan pollInterval,
        CancellationToken ct = default)
    {
        var db = mux.GetDatabase();
        var startedAtUtc = DateTime.UtcNow;
        var deadline = startedAtUtc + maxWait;
        var timeline = new List<CompletionSample>();

        while (DateTime.UtcNow < deadline)
        {
            var completedCount = (int)await db.ListLengthAsync($"bench:{runId}:completions");
            var elapsed = DateTime.UtcNow - startedAtUtc;
            timeline.Add(new CompletionSample(elapsed, completedCount));

            if (completedCount >= totalExpected)
            {
                break;
            }

            await Task.Delay(pollInterval, ct);
        }

        return (DateTime.UtcNow - startedAtUtc, timeline);
    }
}
