using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.Models.Attributes;
using TargetTestScheduleApp.Redis;

namespace TargetTestScheduleApp.Handlers;

// Lane set by attribute, so a scenario covers the attribute path, not only workerLane on the call.
[JobMasterDefinitionId("TestApp.BillingLane")]
[JobMasterWorkerLane("billing")]
public sealed class TestAppBillingLaneHandler(IExecutionRecorder recorder) : IJobMasterHandler
{
    public async Task HandleAsync(JobContext job)
    {
        await Task.Delay(TimeSpan.FromMilliseconds(250));

        var testIdentifier = job.Metadata.TryGetStringValue("TestIdentifier");
        if (!string.IsNullOrEmpty(testIdentifier))
        {
            await recorder.RecordAsync(testIdentifier, job.Id, "TestApp.BillingLane");
        }
    }
}
