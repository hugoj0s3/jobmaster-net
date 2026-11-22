using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Background.Cancellation;

namespace JobMaster.Sdk.Abstractions.Background;

internal interface IJobsExecutionEngineFactory
{
    IJobsExecutionEngine GetOrCreate(IJobMasterBackgroundAgentWorker backgroundAgentWorker, StopControl runnerStopControl, JobMasterPriority priority, string bucketId);
    IJobsExecutionEngine? Get(string bucketId);
}
