using System.Collections.Concurrent;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Background;

namespace JobMaster.Sdk.Background.Runners.JobsExecution;

internal class JobsExecutionEngineFactory : IJobsExecutionEngineFactory
{
    private readonly ConcurrentDictionary<string, IJobsExecutionEngine> engines = new();
    
    public IJobsExecutionEngine GetOrCreate(IJobMasterBackgroundAgentWorker backgroundAgentWorker, JobMasterPriority priority, string bucketId)
    {
        var key = GetKey(priority, bucketId);
        
        return engines.GetOrAdd(key, _ => new JobsExecutionEngine(backgroundAgentWorker, bucketId, priority));
    }
    
    public IJobsExecutionEngine? Get(string bucketId)
    {
        return engines.TryGetValue(bucketId, out var engine) ? engine : null;
    }
    
    private static string GetKey(JobMasterPriority priority, string bucketId)
    {
        return $"{bucketId}";
    }
}
