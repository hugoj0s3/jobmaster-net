using JobMaster.Sdk.Contracts.Ioc.Markups;
using JobMaster.Sdk.Contracts.Models.Agents;

namespace JobMaster.Sdk.Contracts.Repositories.Agent;

public interface IAgentJobsDispatcherRepositoryFactory : IJobMasterClusterAwareComponent
{
    IAgentJobsDispatcherRepository GetRepository(AgentConnectionId agentConnectionId);
}