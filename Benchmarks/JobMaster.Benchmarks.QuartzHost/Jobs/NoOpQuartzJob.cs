using JobMaster.Benchmarks.Common.Recording;
using Quartz;

namespace JobMaster.Benchmarks.QuartzHost.Jobs;

/// <summary>
/// A true no-op job: no artificial delay, a single Redis write on completion -- same contract as
/// <c>NoOpBenchmarkHandler</c> in JobMasterHost, so the benchmark isolates each framework's own
/// scheduling/dispatch overhead rather than handler execution time. Quartz's job identity (<see
/// cref="JobKey"/>) is a string, not the <see cref="Guid"/> the benchmark's schedule endpoints hand
/// back to callers -- the Guid is threaded through explicitly via <see cref="JobDataMap"/> at
/// schedule time and read back here.
/// </summary>
public sealed class NoOpQuartzJob(ICompletionRecorder recorder) : IJob
{
    public const string JobIdDataKey = "jobId";

    public Task Execute(IJobExecutionContext context)
    {
        var jobId = Guid.Parse(context.MergedJobDataMap.GetString(JobIdDataKey)!);
        return recorder.RecordCompletionAsync(jobId);
    }
}
