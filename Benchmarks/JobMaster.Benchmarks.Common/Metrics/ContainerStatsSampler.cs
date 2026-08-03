using Docker.DotNet;
using Docker.DotNet.Models;

namespace JobMaster.Benchmarks.Common.Metrics;

/// <summary>
/// Samples Docker's own container-stats/inspect APIs directly (via a plain <see cref="DockerClient"/>,
/// independent of Testcontainers' internals -- no reflection into the library needed, just a fresh
/// client pointed at the same daemon). Captures everything the stats endpoint already exposes in
/// one call: CPU%, memory, network I/O, block I/O, and PID/thread count. A separate, slower
/// `docker inspect` pass captures restart count and OOM-killed state, since worker containers are
/// capped at 512MB and an OOM kill under the heavier tiers is a real, expected-to-matter failure
/// mode worth surfacing explicitly.
/// </summary>
public sealed class ContainerStatsSampler(DockerClient dockerClient)
{
    public async Task<ContainerStatsSample?> SampleStatsAsync(string containerId, string containerName, CancellationToken ct = default)
    {
        ContainerStatsResponse? stats = null;
        var progress = new Progress<ContainerStatsResponse>(s => stats = s);

        await dockerClient.Containers.GetContainerStatsAsync(
            containerId,
            new ContainerStatsParameters { Stream = false },
            progress,
            ct);

        if (stats is null)
        {
            return null;
        }

        var cpuDelta = (double)stats.CPUStats.CPUUsage.TotalUsage - stats.PreCPUStats.CPUUsage.TotalUsage;
        var systemDelta = (double)stats.CPUStats.SystemUsage - stats.PreCPUStats.SystemUsage;
        var onlineCpus = stats.CPUStats.OnlineCPUs > 0
            ? stats.CPUStats.OnlineCPUs
            : (uint)Math.Max(1, stats.CPUStats.CPUUsage.PercpuUsage?.Count ?? 1);
        var cpuPercent = systemDelta > 0 && cpuDelta > 0 ? cpuDelta / systemDelta * onlineCpus * 100.0 : 0.0;

        long rxBytes = 0, txBytes = 0;
        if (stats.Networks is not null)
        {
            foreach (var network in stats.Networks.Values)
            {
                rxBytes += (long)network.RxBytes;
                txBytes += (long)network.TxBytes;
            }
        }

        long blockReadBytes = 0, blockWriteBytes = 0;
        if (stats.BlkioStats?.IoServiceBytesRecursive is not null)
        {
            foreach (var entry in stats.BlkioStats.IoServiceBytesRecursive)
            {
                if (string.Equals(entry.Op, "read", StringComparison.OrdinalIgnoreCase))
                {
                    blockReadBytes += (long)entry.Value;
                }
                else if (string.Equals(entry.Op, "write", StringComparison.OrdinalIgnoreCase))
                {
                    blockWriteBytes += (long)entry.Value;
                }
            }
        }

        return new ContainerStatsSample(
            TimestampUtc: DateTime.UtcNow,
            ContainerName: containerName,
            CpuPercent: cpuPercent,
            MemoryUsageBytes: (long)stats.MemoryStats.Usage,
            MemoryLimitBytes: (long)stats.MemoryStats.Limit,
            NetworkRxBytes: rxBytes,
            NetworkTxBytes: txBytes,
            BlockReadBytes: blockReadBytes,
            BlockWriteBytes: blockWriteBytes,
            PidsCurrent: (long)(stats.PidsStats?.Current ?? 0));
    }

    public async Task<ContainerHealthSample> SampleHealthAsync(string containerId, string containerName, CancellationToken ct = default)
    {
        var inspect = await dockerClient.Containers.InspectContainerAsync(containerId, ct);

        return new ContainerHealthSample(
            TimestampUtc: DateTime.UtcNow,
            ContainerName: containerName,
            RestartCount: inspect.RestartCount,
            OomKilled: inspect.State.OOMKilled,
            Status: inspect.State.Status);
    }
}
