using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.Agents;

namespace JobMaster.Sdk.Abstractions.Repositories.Agent;

internal interface IAgentComponentFactory : IJobMasterClusterAwareComponent
{
    IAgentJobsDispatcherRepository GetRepository(AgentConnectionId agentConnectionId);
    IAgentFootprintResolver GetFootprintResolver(AgentConnectionId agentConnectionId);
}