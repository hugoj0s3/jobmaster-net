using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.Models.Attributes;
using TargetTestScheduleApp.Redis;

namespace TargetTestScheduleApp.Handlers;

[JobMasterDefinitionId("TestApp.Fast")]
public sealed class TestAppFastHandler(IExecutionRecorder recorder) : IJobMasterHandler
{
    public async Task HandleAsync(JobContext job)
    {
        await Task.Delay(TimeSpan.FromMilliseconds(250));

        // Opt-in only (InjectFailure metadata flag): fails the first attempt then succeeds on retry,
        // so scenarios that need a real JobExecution-category log (only ever written on a failed/retried
        // attempt -- see JobsExecutionEngine.HandleErrorAsync) can get one without every "fast" job
        // scenario paying for it.
        if (job.Metadata.TryGetBoolValue("InjectFailure") == true && job.NumberOfFailures == 0)
        {
            throw new InvalidOperationException("Scenario-injected failure (InjectFailure metadata flag) on first attempt.");
        }

        var testIdentifier = job.Metadata.TryGetStringValue("TestIdentifier");
        if (!string.IsNullOrEmpty(testIdentifier))
        {
            await recorder.RecordAsync(testIdentifier, job.Id, "TestApp.Fast");
        }
    }
}
