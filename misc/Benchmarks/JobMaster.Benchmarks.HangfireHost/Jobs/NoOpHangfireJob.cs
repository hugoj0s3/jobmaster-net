using JobMaster.Benchmarks.Common.Recording;

namespace JobMaster.Benchmarks.HangfireHost.Jobs;

/// <summary>
/// Simulates a real job body (see <see cref="SimulatedJobWork"/>) then a single Redis write on
/// completion -- same contract as NoOpBenchmarkHandler (JobMasterHost) and NoOpQuartzJob
/// (QuartzHost). Hangfire's own job ID is a string backed by a bigint counter, not a Guid -- the
/// benchmark's own Guid is threaded through explicitly as a method parameter (captured in the
/// job's serialized invocation data) rather than relying on Hangfire's native ID, same rationale
/// as Quartz's JobDataMap threading.
/// </summary>
public sealed class NoOpHangfireJob(ICompletionRecorder recorder)
{
    public async Task Execute(Guid jobId)
    {
        await SimulatedJobWork.DelayAsync();
        await recorder.RecordCompletionAsync(jobId);
    }
}
