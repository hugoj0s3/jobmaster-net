using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.Sdk.Abstractions.Services.Master;

namespace JobMaster.Sdk.Repositories;

internal sealed class SingleAgentJobsDispatcherRepository : AgentJobsDispatcherRepository
{
    public SingleAgentJobsDispatcherRepository(
        JobMasterClusterConnectionConfig clusterConnConfig,
        string agentRepoTypeId,
        IAgentRawMessagesDispatcherRepository repository,
        IJobMasterLogger logger)
        : base(clusterConnConfig, agentRepoTypeId, repository, repository, logger)
    {
    }
}
