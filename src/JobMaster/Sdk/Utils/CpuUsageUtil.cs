using System.Diagnostics;

namespace JobMaster.Sdk.Utils;

internal static class CpuUsageUtil
{
    // Normalized to 0..100% for the whole machine.
    internal static async Task<double> GetProcessCpuPercentTotalAsync(
        Process? process = null,
        int sampleMs = 500,
        CancellationToken ct = default)
    {
        var perCore = await GetProcessCpuPercentPerCoreAsync(process, sampleMs, ct).ConfigureAwait(false);
        return perCore / Environment.ProcessorCount;
    }
    
    // % of one logical CPU. Can exceed 100 on multi-core machines.
    private static async Task<double> GetProcessCpuPercentPerCoreAsync(
        Process? process = null,
        int sampleMs = 500,
        CancellationToken ct = default)
    {
        process ??= Process.GetCurrentProcess();

        process.Refresh();
        var startCpu = process.TotalProcessorTime;
        var startTs = Stopwatch.GetTimestamp();

        await Task.Delay(sampleMs, ct).ConfigureAwait(false);

        process.Refresh();
        var endCpu = process.TotalProcessorTime;
        var endTs = Stopwatch.GetTimestamp();

        var cpuMs = (endCpu - startCpu).TotalMilliseconds;
        var elapsedMs = (endTs - startTs) * 1000.0 / Stopwatch.Frequency;

        if (elapsedMs <= 0) return 0;

        return (cpuMs / elapsedMs) * 100.0;
    }
}