using JobMaster.Abstractions.Models;

namespace JobMaster.Abstractions.Ioc.Selectors;

public interface IAgentWorkerSelector
{
    IAgentWorkerSelector AgentConnName(string agentConnName);
    IAgentWorkerSelector WorkerName(string workerName);
    IAgentWorkerSelector WorkerLane(string workerLane);
    IAgentWorkerSelector TransferBatchSize(int batchSize);
    IAgentWorkerSelector BucketBufferSize(int bufferSize);
    IAgentWorkerSelector BucketBufferLeadTime(TimeSpan bucketDuration);
    IAgentWorkerSelector BucketQtyConfig(JobMasterPriority priority, int qty);
    IAgentWorkerSelector SetWorkerMode(AgentWorkerMode mode);
    IAgentWorkerSelector SkipWarmUpTime();
    IAgentWorkerSelector ParallelismFactor(double factor);
}