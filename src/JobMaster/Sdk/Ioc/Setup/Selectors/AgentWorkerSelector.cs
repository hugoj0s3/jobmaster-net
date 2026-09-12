using JobMaster.Abstractions.Ioc.Selectors;
using JobMaster.Abstractions.Models;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Ioc.Definitions;

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

    public IAgentWorkerSelector TransferBatchSize(int batchSize)
    {
        worker.TransferBatchSize = batchSize;
        return this;
    }

    public IAgentWorkerSelector BucketBufferSize(int bufferSize)
    {
        worker.BucketBufferSize = bufferSize;
        return this;
    }

    public IAgentWorkerSelector BucketBufferLeadTime(TimeSpan leadTime)
    {
        if (leadTime > JobMasterConstants.OnBoardingWindow)
        {
            throw new ArgumentException($"buffet buffer lead time must be greater than: {JobMasterConstants.OnBoardingWindow}");
        }

        if (leadTime < TimeSpan.FromMilliseconds(250))
        {
            throw new ArgumentException($"buffer lead time must be lower than: {TimeSpan.FromMilliseconds(250)}");
        }
        
        worker.BucketBufferLeadTime = leadTime;
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
    
    public IAgentWorkerSelector ParallelismFactor(double factor)
    {
        worker.ParallelismFactor = factor;
        return this;
    }
}