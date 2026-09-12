using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.Models.Attributes;
using TargetTestRecurringApp.Redis;

namespace TargetTestRecurringApp.Handlers;

[JobMasterDefinitionId("RecurringApp.Tick")]
public sealed class RecurringTickHandler(IExecutionRecorder recorder) : IJobMasterHandler
{
    public async Task HandleAsync(JobContext job)
    {
        var testIdentifier = job.Metadata.TryGetStringValue("TestIdentifier");
        if (!string.IsNullOrEmpty(testIdentifier))
        {
            await recorder.RecordAsync(testIdentifier, job.Id, "RecurringApp.Tick");
        }
    }
}
