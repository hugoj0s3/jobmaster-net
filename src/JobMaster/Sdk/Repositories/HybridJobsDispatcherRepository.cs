using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.Sdk.Abstractions.Services.Master;

namespace JobMaster.Sdk.Repositories;

internal sealed class HybridJobsDispatcherRepository : AgentJobsDispatcherRepository
{
    public HybridJobsDispatcherRepository(
        JobMasterClusterConnectionConfig clusterConnConfig,
        string agentRepoTypeId,
        IAgentRawMessagesDispatcherRepository savePendingRepository,
        IAgentRawMessagesDispatcherRepository processingRepository,
        IJobMasterLogger logger)
        : base(clusterConnConfig, agentRepoTypeId, savePendingRepository, processingRepository, logger)
    {
    }
}
