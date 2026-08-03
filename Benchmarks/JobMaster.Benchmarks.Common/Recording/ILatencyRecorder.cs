namespace JobMaster.Benchmarks.Common.Recording;

/// <summary>Written by the load generator (Runner side) at enqueue time -- the expected-time hash
/// this produces also doubles as the full scheduled-job-ID set used for lost-job detection by
/// <see cref="LatencyJoiner"/>.</summary>
public interface ILatencyRecorder
{
    Task RecordExpectedAsync(Guid jobId, DateTime expectedAtUtc, bool isImmediate, CancellationToken ct = default);
}
