namespace JobMaster.Benchmarks.Common.Metrics;

public sealed record ContainerStatsSample(
    DateTime TimestampUtc,
    string ContainerName,
    double CpuPercent,
    long MemoryUsageBytes,
    long MemoryLimitBytes,
    long NetworkRxBytes,
    long NetworkTxBytes,
    long BlockReadBytes,
    long BlockWriteBytes,
    long PidsCurrent);

public sealed record ContainerHealthSample(
    DateTime TimestampUtc,
    string ContainerName,
    long RestartCount,
    bool OomKilled,
    string Status);
