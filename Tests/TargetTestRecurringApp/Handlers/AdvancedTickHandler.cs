using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using TargetTestRecurringApp.Redis;

namespace TargetTestRecurringApp.Handlers;

public sealed class AdvancedTickDefinitionAttribute : JobDefinitionConfigAttribute
{
    public override JobDefinitionConfig Config { get; } = new JobDefinitionConfig("RecurringApp.AdvancedTick");
}

[AdvancedTickDefinitionAttribute]
public sealed class AdvancedTickHandler(IExecutionRecorder recorder) : IJobMasterHandler
{
    public async Task HandleAsync(JobContext job)
    {
        var testIdentifier = job.Metadata.TryGetStringValue("TestIdentifier");
        if (!string.IsNullOrEmpty(testIdentifier))
        {
            await recorder.RecordAsync(testIdentifier, job.Id, "RecurringApp.AdvancedTick");
        }
    }
}
