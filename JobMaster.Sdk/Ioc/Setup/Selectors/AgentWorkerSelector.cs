using JobMaster.Contracts.Ioc.Selectors;
using JobMaster.Contracts.Models;
using JobMaster.Sdk.Contracts.Ioc.Definitions;

namespace JobMaster.Sdk.Ioc.Setup.Selectors;

internal sealed class AgentWorkerSelector : IAgentWorkerSelector
{
    private readonly ClusterConfigBuilder root;
    private readonly WorkerDefinition worker;
    public AgentWorkerSelector(ClusterConfigBuilder root, WorkerDefinition worker)
    {
        this.root = root;
        this.worker = worker;
    }

    public IAgentWorkerSelector AgentConnName(string agentConnName)
    {
        worker.AgentConnectionName = agentConnName;
        return this;
    }

    public IAgentWorkerSelector WorkerName(string workerName)
    {
        worker.WorkerName = workerName;
        return this;
    }

    public IAgentWorkerSelector WorkerLane(string workerLane)
    {
        worker.WorkerLane = workerLane;
        return this;
    }

    public IAgentWorkerSelector WorkerBatchSize(int batchSize = 250)
    {
        worker.BatchSize = batchSize;
        return this;
    }

    public IAgentWorkerSelector BucketQtyConfig(JobMasterPriority priority, int qty)
    {
        worker.BucketQty[priority] = qty;
        return this;
    }
    
    public IAgentWorkerSelector SetWorkerMode(AgentWorkerMode mode)
    {
        worker.Mode = mode;
        return this;
    }

    public IAgentWorkerSelector SkipWarmUpTime()
    {
        worker.SkipWarmUpTime = true;
        return this;
    }
}