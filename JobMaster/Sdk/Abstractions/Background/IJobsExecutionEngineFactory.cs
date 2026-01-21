using System.ComponentModel;
using JobMaster.Abstractions.Models;

namespace JobMaster.Sdk.Abstractions.Background;

[EditorBrowsable(EditorBrowsableState.Never)]
public interface IJobsExecutionEngineFactory
{
    IJobsExecutionEngine GetOrCreate(IJobMasterBackgroundAgentWorker backgroundAgentWorker, JobMasterPriority priority, string bucketId);
    IJobsExecutionEngine? Get(string bucketId);
}
