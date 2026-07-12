using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.Models.Attributes;
using TargetTestScheduleApp.Redis;

namespace TargetTestScheduleApp.Handlers;

[JobMasterDefinitionId("TestApp.Slow")]
public sealed class TestAppSlowHandler(IExecutionRecorder recorder) : IJobMasterHandler
{
    public async Task HandleAsync(JobContext job)
    {
        await Task.Delay(TimeSpan.FromSeconds(10));

        var testIdentifier = job.Metadata.TryGetStringValue("TestIdentifier");
        if (!string.IsNullOrEmpty(testIdentifier))
        {
            await recorder.RecordAsync(testIdentifier, job.Id, "TestApp.Slow");
        }
    }
}
