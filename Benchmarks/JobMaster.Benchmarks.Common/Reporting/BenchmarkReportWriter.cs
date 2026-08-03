using System.Globalization;
using System.Text;
using JobMaster.Benchmarks.Common.Metrics;
using JobMaster.Benchmarks.Common.Recording;

namespace JobMaster.Benchmarks.Common.Reporting;

/// <summary>Writes the raw per-container metric time series to CSV (for later cross-run charting)
/// and a human-readable Markdown summary (throughput/latency/correctness/resource-usage overview)
/// for a single benchmark run.</summary>
public sealed class BenchmarkReportWriter
{
    public async Task WriteAsync(
        string outputDirectory,
        BenchmarkRunMetadata metadata,
        LatencyReport latency,
        ScheduleCallLatencyReport scheduleCallLatency,
        IReadOnlyList<ContainerStatsSample> statsSamples,
        IReadOnlyList<ContainerHealthSample> healthSamples,
        IReadOnlyList<CompletionSample>? completionTimeline = null,
        CancellationToken ct = default)
    {
        Directory.CreateDirectory(outputDirectory);

        await WriteStatsCsvAsync(Path.Combine(outputDirectory, "container-stats.csv"), statsSamples, ct);
        await WriteHealthCsvAsync(Path.Combine(outputDirectory, "container-health.csv"), healthSamples, ct);
        await WriteLostDuplicatedCsvAsync(Path.Combine(outputDirectory, "lost-duplicated-jobs.csv"), latency, ct);
        await WriteCompletionTimelineCsvAsync(Path.Combine(outputDirectory, "completion-timeline.csv"), completionTimeline ?? [], ct);
        await WriteSummaryMarkdownAsync(Path.Combine(outputDirectory, "summary.md"), metadata, latency, scheduleCallLatency, statsSamples, ct);
    }

    // Completed-count-over-time curve, sampled during the post-load-generation drain wait (both
    // paced and burst tests use BurstCompletionWaiter for this now) -- shows ramp-up/steady-state/
    // drain-off shape rather than just a single "total drain time" number.
    private static async Task WriteCompletionTimelineCsvAsync(string path, IReadOnlyList<CompletionSample> timeline, CancellationToken ct)
    {
        var sb = new StringBuilder();
        sb.AppendLine("ElapsedSeconds,CompletedCount");
        foreach (var sample in timeline)
        {
            sb.AppendLine($"{sample.Elapsed.TotalSeconds.ToString("F1", CultureInfo.InvariantCulture)},{sample.CompletedCount}");
        }

        await File.WriteAllTextAsync(path, sb.ToString(), ct);
    }

    private static async Task WriteStatsCsvAsync(string path, IReadOnlyList<ContainerStatsSample> samples, CancellationToken ct)
    {
        var sb = new StringBuilder();
        sb.AppendLine("TimestampUtc,ContainerName,CpuPercent,MemoryUsageBytes,MemoryLimitBytes,NetworkRxBytes,NetworkTxBytes,BlockReadBytes,BlockWriteBytes,PidsCurrent");
        foreach (var s in samples)
        {
            sb.AppendLine(string.Join(',',
                s.TimestampUtc.ToString("O", CultureInfo.InvariantCulture),
                s.ContainerName,
                s.CpuPercent.ToString("F2", CultureInfo.InvariantCulture),
                s.MemoryUsageBytes,
                s.MemoryLimitBytes,
                s.NetworkRxBytes,
                s.NetworkTxBytes,
                s.BlockReadBytes,
                s.BlockWriteBytes,
                s.PidsCurrent));
        }

        await File.WriteAllTextAsync(path, sb.ToString(), ct);
    }

    private static async Task WriteHealthCsvAsync(string path, IReadOnlyList<ContainerHealthSample> samples, CancellationToken ct)
    {
        var sb = new StringBuilder();
        sb.AppendLine("TimestampUtc,ContainerName,RestartCount,OomKilled,Status");
        foreach (var s in samples)
        {
            sb.AppendLine(string.Join(',',
                s.TimestampUtc.ToString("O", CultureInfo.InvariantCulture),
                s.ContainerName,
                s.RestartCount,
                s.OomKilled,
                s.Status));
        }

        await File.WriteAllTextAsync(path, sb.ToString(), ct);
    }

    private static async Task WriteLostDuplicatedCsvAsync(string path, LatencyReport latency, CancellationToken ct)
    {
        var sb = new StringBuilder();
        sb.AppendLine("JobId,Kind");
        foreach (var id in latency.LostJobIds)
        {
            sb.AppendLine($"{id},Lost");
        }

        foreach (var id in latency.DuplicatedJobIds)
        {
            sb.AppendLine($"{id},Duplicated");
        }

        await File.WriteAllTextAsync(path, sb.ToString(), ct);
    }

    private static async Task WriteSummaryMarkdownAsync(
        string path,
        BenchmarkRunMetadata metadata,
        LatencyReport latency,
        ScheduleCallLatencyReport scheduleCallLatency,
        IReadOnlyList<ContainerStatsSample> statsSamples,
        CancellationToken ct)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"# Benchmark: {metadata.Framework} / {metadata.DbConfig} ({metadata.TestType})");
        sb.AppendLine();
        if (metadata.TestType == "Burst")
        {
            sb.AppendLine($"- Total burst jobs: {metadata.TotalBurstJobs}");
            var immediateRequestCount = scheduleCallLatency.Immediate.SampleCount;
            if (immediateRequestCount > 0 && metadata.TotalBurstJobs is { } totalBurstJobs)
            {
                var batchSize = totalBurstJobs / immediateRequestCount;
                sb.AppendLine($"- Requests: {immediateRequestCount} x {batchSize} jobs each (all fired in parallel)");
            }
        }
        else
        {
            sb.AppendLine($"- Target rate: {metadata.TargetJobsPerMinute} jobs/min");
            sb.AppendLine($"- Duration: {metadata.Duration}");
            sb.AppendLine("- Jobs per request: 1");
        }
        sb.AppendLine($"- Workers: {metadata.WorkerCount}");
        sb.AppendLine($"- Started: {metadata.StartedAtUtc:O}");
        sb.AppendLine($"- Completed: {metadata.CompletedAtUtc:O}");
        sb.AppendLine();
        sb.AppendLine("## Throughput & correctness");
        sb.AppendLine();

        if (metadata.FailedRequestCount > 0)
        {
            // Surfaced separately from Lost/Duplicated -- these are schedule HTTP calls that never
            // got a response at all (timeout, connection reset), which otherwise makes TotalScheduled
            // look like a near-zero/broken run instead of "most requests succeeded, N did not."
            sb.AppendLine($"- **{metadata.FailedRequestCount} schedule request(s) failed** (HTTP timeout/connection error -- see console/log output for detail; jobs in a failed request are not counted in Total scheduled below)");
        }

        if (metadata.TestType == "Burst")
        {
            sb.AppendLine($"- Total scheduled: {latency.TotalScheduled}");

            // All batches fire concurrently in burst mode (Task.WhenAll, not sequential), so the
            // total wall-clock time to schedule everything is governed by whichever single batch
            // took longest to come back, not the sum of all of them -- the max schedule-call latency
            // IS that duration.
            var schedulingDurationMs = Math.Max(scheduleCallLatency.Immediate.MaxMs, scheduleCallLatency.Delayed.MaxMs);
            if (schedulingDurationMs > 0)
            {
                var schedulingThroughputPerSecond = latency.TotalScheduled / (schedulingDurationMs / 1000.0);
                sb.AppendLine($"- Scheduling throughput: {schedulingThroughputPerSecond:F0} jobs/sec (all {latency.TotalScheduled} jobs scheduled in parallel, done in {TimeSpan.FromMilliseconds(schedulingDurationMs)})");
            }
        }
        else
        {
            var loadWindowSeconds = metadata.Duration.TotalSeconds;
            var scheduledPerSecond = latency.TotalScheduled / loadWindowSeconds;
            sb.AppendLine($"- Total scheduled: {latency.TotalScheduled} ({scheduledPerSecond:F2} jobs/sec -- confirms the load generator achieved its configured target pace, this is an input check, not a measured ceiling)");
        }

        // Completions are NOT confined to the load window -- delayed jobs scheduled near the end of
        // it are due up to DelayMax later, plus whatever execution latency stacks on top, so real
        // completions trail off well past metadata.Duration. Dividing by the load-window duration
        // (as an earlier version of this report did) silently just reprints the input rate; the
        // actual sustained completion throughput is total completed over the real span from run
        // start to the last observed completion.
        if (latency.LastCompletionAtUtc is { } lastCompletion && latency.TotalCompletedJobs > 0)
        {
            var actualSpanSeconds = (lastCompletion - metadata.StartedAtUtc).TotalSeconds;
            var completedPerSecond = actualSpanSeconds > 0 ? latency.TotalCompletedJobs / actualSpanSeconds : 0;
            sb.AppendLine($"- Total completed (distinct jobs): {latency.TotalCompletedJobs} ({completedPerSecond:F2} jobs/sec, measured over the actual {TimeSpan.FromSeconds(actualSpanSeconds)} span from run start to the last real completion)");
        }
        else
        {
            sb.AppendLine($"- Total completed (distinct jobs): {latency.TotalCompletedJobs}");
        }

        sb.AppendLine($"- **Lost**: {latency.LostCount}");
        sb.AppendLine($"- **Duplicated**: {latency.DuplicatedCount}");
        sb.AppendLine();
        sb.AppendLine("## Schedule-call latency (time for the schedule HTTP call itself to return)");
        sb.AppendLine();
        sb.AppendLine("| | mean (ms) | p50 (ms) | p90 (ms) | p99 (ms) | max (ms) | samples |");
        sb.AppendLine("|---|---|---|---|---|---|---|");
        sb.AppendLine(FormatPercentileRow("schedule-now call", scheduleCallLatency.Immediate));
        // Burst mode is always all-immediate (no delayed jobs requested at all) -- omit a row that
        // would otherwise always read as a meaningless all-zero line.
        if (scheduleCallLatency.Delayed.SampleCount > 0)
        {
            sb.AppendLine(FormatPercentileRow("schedule-after call", scheduleCallLatency.Delayed));
        }
        sb.AppendLine();
        sb.AppendLine("## Execution latency (due time to actual execution)");
        sb.AppendLine();
        sb.AppendLine("| | mean (ms) | p50 (ms) | p90 (ms) | p99 (ms) | max (ms) | samples |");
        sb.AppendLine("|---|---|---|---|---|---|---|");
        sb.AppendLine(FormatPercentileRow("Immediate", latency.Immediate));
        sb.AppendLine(FormatPercentileRow("Delayed", latency.Delayed));
        sb.AppendLine();
        sb.AppendLine("## Container resource usage (summary across the run)");
        sb.AppendLine();
        sb.AppendLine("| Container | avg CPU% | max CPU% | avg mem (MB) | max mem (MB) | total net rx/tx (MB) | total block r/w (MB) | max PIDs |");
        sb.AppendLine("|---|---|---|---|---|---|---|---|");

        foreach (var group in statsSamples.GroupBy(s => s.ContainerName))
        {
            var list = group.ToList();
            var avgCpu = list.Average(s => s.CpuPercent);
            var maxCpu = list.Max(s => s.CpuPercent);
            var avgMemMb = list.Average(s => s.MemoryUsageBytes) / (1024.0 * 1024.0);
            var maxMemMb = list.Max(s => s.MemoryUsageBytes) / (1024.0 * 1024.0);
            var totalRxMb = list.Max(s => s.NetworkRxBytes) / (1024.0 * 1024.0);
            var totalTxMb = list.Max(s => s.NetworkTxBytes) / (1024.0 * 1024.0);
            var totalReadMb = list.Max(s => s.BlockReadBytes) / (1024.0 * 1024.0);
            var totalWriteMb = list.Max(s => s.BlockWriteBytes) / (1024.0 * 1024.0);
            var maxPids = list.Max(s => s.PidsCurrent);

            sb.AppendLine(
                $"| {group.Key} | {avgCpu:F1} | {maxCpu:F1} | {avgMemMb:F1} | {maxMemMb:F1} | " +
                $"{totalRxMb:F1}/{totalTxMb:F1} | {totalReadMb:F1}/{totalWriteMb:F1} | {maxPids} |");
        }

        await File.WriteAllTextAsync(path, sb.ToString(), ct);
    }

    private static string FormatPercentileRow(string label, LatencyPercentiles p) =>
        $"| {label} | {p.MeanMs:F0} | {p.P50Ms:F0} | {p.P90Ms:F0} | {p.P99Ms:F0} | {p.MaxMs:F0} | {p.SampleCount} |";
}
