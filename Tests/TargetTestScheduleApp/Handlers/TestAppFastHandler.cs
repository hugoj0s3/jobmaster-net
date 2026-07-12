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

        var testIdentifier = job.Metadata.TryGetStringValue("TestIdentifier");
        if (!string.IsNullOrEmpty(testIdentifier))
        {
            await recorder.RecordAsync(testIdentifier, job.Id, "TestApp.Fast");
        }
    }
}
