using System.Collections.Concurrent;

namespace JobMaster.Benchmarks.Common.Recording;

/// <summary>
/// In-process (no Redis round-trip needed) collector for how long each schedule HTTP call itself
/// takes to return, measured by <see cref="Load.LoadGenerator"/> around every
/// <c>ScheduleNowAsync</c>/<c>ScheduleAfterAsync</c> call. Distinct from <see cref="LatencyJoiner"/>,
/// which measures end-to-end due-time-to-execution latency after the run -- this measures the
/// framework's raw enqueue-acceptance speed while the run is in flight.
/// </summary>
public sealed class ScheduleCallLatencyRecorder
{
    private readonly ConcurrentBag<double> _immediateMs = [];
    private readonly ConcurrentBag<double> _delayedMs = [];

    public void RecordImmediate(double elapsedMs) => _immediateMs.Add(elapsedMs);

    public void RecordDelayed(double elapsedMs) => _delayedMs.Add(elapsedMs);

    public ScheduleCallLatencyReport Compute() => new(
        Immediate: LatencyPercentiles.FromMilliseconds(_immediateMs),
        Delayed: LatencyPercentiles.FromMilliseconds(_delayedMs));
}
