using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.Agents;

namespace JobMaster.Sdk.Abstractions.Repositories.Agent;

internal interface IAgentJobsDispatcherRepositoryFactory : IJobMasterClusterAwareComponent
{
    IAgentJobsDispatcherRepository GetRepository(AgentConnectionId agentConnectionId);
}