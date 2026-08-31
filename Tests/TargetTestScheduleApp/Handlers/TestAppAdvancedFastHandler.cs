using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using TargetTestScheduleApp.Redis;

namespace TargetTestScheduleApp.Handlers;

public sealed class TestAppAdvancedFastDefinitionAttribute : JobDefinitionConfigAttribute, IStaticJobDefinitionConfig
{
    public static JobDefinitionConfig Config { get; } = new JobDefinitionConfig("TestApp.AdvancedFast");
}

[TestAppAdvancedFastDefinitionAttribute]
public sealed class TestAppAdvancedFastHandler(IExecutionRecorder recorder) : IJobMasterHandler
{
    public async Task HandleAsync(JobContext job)
    {
        await Task.Delay(TimeSpan.FromMilliseconds(250));

        var testIdentifier = job.Metadata.TryGetStringValue("TestIdentifier");
        if (!string.IsNullOrEmpty(testIdentifier))
        {
            await recorder.RecordAsync(testIdentifier, job.Id, "TestApp.AdvancedFast");
        }
    }
}
