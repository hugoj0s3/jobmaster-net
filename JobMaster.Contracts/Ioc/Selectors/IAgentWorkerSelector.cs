using JobMaster.Contracts.Models;

namespace JobMaster.Contracts.Ioc.Selectors;

public interface IAgentWorkerSelector
{
    IAgentWorkerSelector AgentConnName(string agentConnName);
    IAgentWorkerSelector WorkerName(string workerName);
    IAgentWorkerSelector WorkerLane(string workerLane);
    IAgentWorkerSelector WorkerBatchSize(int batchSize = 250);
    IAgentWorkerSelector BucketQtyConfig(JobMasterPriority priority, int qty);
    IAgentWorkerSelector SetWorkerMode(AgentWorkerMode mode);
    IAgentWorkerSelector SkipWarmUpTime();
}