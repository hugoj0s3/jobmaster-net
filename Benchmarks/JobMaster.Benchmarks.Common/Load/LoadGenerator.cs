using System.Diagnostics;
using JobMaster.Benchmarks.Common.Recording;
using JobMaster.Benchmarks.Common.Scheduling;

namespace JobMaster.Benchmarks.Common.Load;

/// <summary>
/// Drives scheduling load against an <see cref="IScheduleClient"/> for the configured duration.
/// Tick interval is adaptive per tier (see <see cref="LoadGeneratorOptions.TargetJobsPerTick"/>) so
/// higher target rates get finer, more frequent ticks instead of one giant burst every few seconds
/// -- at a fixed 5s tick, the 100k-jobs/min tier would fire ~8,333 jobs as a single mega-burst,
/// which doesn't resemble real traffic. Jobs within a tick fire as bounded-concurrency parallel
/// requests, not a sequential loop, to simulate real concurrent client load.
/// </summary>
public sealed class LoadGenerator(
    IScheduleClient client,
    ILatencyRecorder latencyRecorder,
    LoadGeneratorOptions options,
    ScheduleCallLatencyRecorder scheduleCallLatencyRecorder)
{
    private int failedRequestCount;

    // Requests whose HTTP call itself failed (timeout, connection reset), distinct from jobs lost
    // after a successful schedule call.
    public int FailedRequestCount => failedRequestCount;

    public async Task RunAsync(CancellationToken ct = default)
    {
        if (options.BurstTotalJobs is { } totalJobs)
        {
            await RunBurstAsync(totalJobs, ct);
            return;
        }

        // Tick interval is sized off the higher of the two rates (primary vs. step-down) so it
        // stays fine-grained enough for whichever phase is busiest -- jobsPerTick is recomputed
        // every tick from whichever rate is currently active, using this same fixed interval.
        var peakRatePerMinute = Math.Max(options.TargetJobsPerMinute, options.StepDownJobsPerMinute ?? 0);
        var tickInterval = ComputeTickInterval(peakRatePerMinute / 60.0);

        using var semaphore = new SemaphoreSlim(options.MaxConcurrentRequests);
        var random = Random.Shared;
        var startedAtUtc = DateTime.UtcNow;
        var stopAtUtc = startedAtUtc + options.Duration;
        using var timer = new PeriodicTimer(tickInterval);

        var inFlight = new List<Task>();

        while (DateTime.UtcNow < stopAtUtc && await timer.WaitForNextTickAsync(ct))
        {
            var elapsed = DateTime.UtcNow - startedAtUtc;
            var isStepDown = options.StepDownJobsPerMinute.HasValue && options.StepDownAt.HasValue && elapsed >= options.StepDownAt.Value;
            var currentRatePerMinute = isStepDown ? options.StepDownJobsPerMinute!.Value : options.TargetJobsPerMinute;
            var currentRatePerSecond = currentRatePerMinute / 60.0;
            var jobsPerTick = Math.Max(1, (int)Math.Round(currentRatePerSecond * tickInterval.TotalSeconds));

            // Once stepped down, only immediate jobs are scheduled -- delayed jobs from this phase
            // would need 5-30 more minutes just to become due, adding nothing to what the step-down
            // phase is meant to observe (how fast the backlog from the peak phase drains) while
            // dragging out the grace period needed at the end of the run.
            var currentImmediateFraction = isStepDown ? 1.0 : options.ImmediateFraction;
            var immediateCount = (int)Math.Round(jobsPerTick * currentImmediateFraction);
            var delayedCount = jobsPerTick - immediateCount;

            foreach (var batch in ChunksOf(immediateCount, options.JobsPerRequest))
            {
                inFlight.Add(FireImmediateAsync(batch, semaphore, ct));
            }

            foreach (var batch in ChunksOf(delayedCount, options.JobsPerRequest))
            {
                var delay = RandomDelay(random);
                inFlight.Add(FireDelayedAsync(batch, delay, semaphore, ct));
            }

            inFlight.RemoveAll(t => t.IsCompleted);
        }

        await Task.WhenAll(inFlight);
    }

    // No pacing at all -- every job is immediate, fired in batches of BurstBatchSize as fast as
    // MaxConcurrentRequests allows. The caller (Program.cs) is responsible for polling completions
    // afterward and measuring the actual drain time; this method's job is only to get every batch
    // fired as quickly as possible.
    private async Task RunBurstAsync(int totalJobs, CancellationToken ct)
    {
        using var semaphore = new SemaphoreSlim(options.MaxConcurrentRequests);
        var batchSize = Math.Max(1, options.BurstBatchSize);
        var tasks = ChunksOf(totalJobs, batchSize).Select(batch => FireImmediateAsync(batch, semaphore, ct));
        await Task.WhenAll(tasks);
    }

    private TimeSpan RandomDelay(Random random)
    {
        var minTicks = options.DelayMin.Ticks;
        var maxTicks = options.DelayMax.Ticks;
        return maxTicks > minTicks
            ? TimeSpan.FromTicks(minTicks + (long)(random.NextDouble() * (maxTicks - minTicks)))
            : options.DelayMin;
    }

    private TimeSpan ComputeTickInterval(double ratePerSecond)
    {
        if (ratePerSecond <= 0)
        {
            return options.MaxTickInterval;
        }

        var seconds = options.TargetJobsPerTick / ratePerSecond;
        var clamped = Math.Clamp(seconds, options.MinTickInterval.TotalSeconds, options.MaxTickInterval.TotalSeconds);
        return TimeSpan.FromSeconds(clamped);
    }

    private async Task FireImmediateAsync(int count, SemaphoreSlim semaphore, CancellationToken ct)
    {
        if (count < 1) return;

        await semaphore.WaitAsync(ct);
        try
        {
            var expectedAtUtc = DateTime.UtcNow;
            var sw = Stopwatch.StartNew();
            var jobIds = await client.ScheduleNowAsync(count, ct);
            sw.Stop();
            scheduleCallLatencyRecorder.RecordImmediate(sw.Elapsed.TotalMilliseconds);
            foreach (var jobId in jobIds)
            {
                await latencyRecorder.RecordExpectedAsync(jobId, expectedAtUtc, isImmediate: true, ct);
            }
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            // Swallowed (not rethrown) so one failed batch doesn't abort Task.WhenAll for the rest.
            // Checking ct.IsCancellationRequested, not exception type -- HttpClient.Timeout's own
            // internal cancellation also throws TaskCanceledException, which "ex is not
            // OperationCanceledException" would wrongly treat as a real cancellation.
            Interlocked.Increment(ref failedRequestCount);
            Console.WriteLine($"Schedule request failed ({count} jobs, now): {ex.GetType().Name}: {ex.Message}");
        }
        finally
        {
            semaphore.Release();
        }
    }

    private async Task FireDelayedAsync(int count, TimeSpan delay, SemaphoreSlim semaphore, CancellationToken ct)
    {
        if (count < 1) return;

        await semaphore.WaitAsync(ct);
        try
        {
            var expectedAtUtc = DateTime.UtcNow + delay;
            var sw = Stopwatch.StartNew();
            var jobIds = await client.ScheduleAfterAsync(count, delay, ct);
            sw.Stop();
            scheduleCallLatencyRecorder.RecordDelayed(sw.Elapsed.TotalMilliseconds);
            foreach (var jobId in jobIds)
            {
                await latencyRecorder.RecordExpectedAsync(jobId, expectedAtUtc, isImmediate: false, ct);
            }
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            Interlocked.Increment(ref failedRequestCount);
            Console.WriteLine($"Schedule request failed ({count} jobs, after {delay}): {ex.GetType().Name}: {ex.Message}");
        }
        finally
        {
            semaphore.Release();
        }
    }

    private static IEnumerable<int> ChunksOf(int total, int chunkSize)
    {
        if (chunkSize < 1) chunkSize = 1;

        var remaining = total;
        while (remaining > 0)
        {
            var chunk = Math.Min(chunkSize, remaining);
            yield return chunk;
            remaining -= chunk;
        }
    }
}
