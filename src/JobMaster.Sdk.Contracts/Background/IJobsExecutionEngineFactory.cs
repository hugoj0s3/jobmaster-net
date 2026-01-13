using JobMaster.Contracts.Models;

namespace JobMaster.Sdk.Contracts.Background;

public interface IJobsExecutionEngineFactory
{
    IJobsExecutionEngine GetOrCreate(IJobMasterBackgroundAgentWorker backgroundAgentWorker, JobMasterPriority priority, string bucketId);
    IJobsExecutionEngine? Get(string bucketId);
}
