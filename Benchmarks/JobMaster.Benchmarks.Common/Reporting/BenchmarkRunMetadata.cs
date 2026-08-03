namespace JobMaster.Benchmarks.Common.Reporting;

public sealed record BenchmarkRunMetadata(
    string Framework,
    string DbConfig,
    int TargetJobsPerMinute,
    int WorkerCount,
    TimeSpan Duration,
    DateTime StartedAtUtc,
    DateTime CompletedAtUtc,
    // "Paced" (default, steady arrival at TargetJobsPerMinute) or "Burst" (flood TotalBurstJobs as
    // fast as possible, TargetJobsPerMinute/Duration are meaningless for that run type).
    string TestType = "Paced",
    int? TotalBurstJobs = null,
    int FailedRequestCount = 0);
