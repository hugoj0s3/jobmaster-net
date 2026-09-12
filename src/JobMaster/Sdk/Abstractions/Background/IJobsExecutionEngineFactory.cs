using JobMaster.Abstractions.Models;

namespace JobMaster.Sdk.Abstractions.Background;

internal interface IJobsExecutionEngineFactory
{
    IJobsExecutionEngine GetOrCreate(IJobMasterBackgroundAgentWorker backgroundAgentWorker, JobMasterPriority priority, string bucketId);
    IJobsExecutionEngine? Get(string bucketId);
}
