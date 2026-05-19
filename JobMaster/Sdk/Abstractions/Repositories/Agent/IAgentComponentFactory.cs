using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.Agents;

namespace JobMaster.Sdk.Abstractions.Repositories.Agent;

internal interface IAgentComponentFactory : IJobMasterClusterAwareComponent
{
    IAgentJobsDispatcherRepository GetRepository(AgentConnectionId agentConnectionId);
    IAgentFingerprintResolver GetFingerprintResolver(AgentConnectionId agentConnectionId);
    
    IAgentJobsDispatcherRepository GetRepository(string agentConnectionId);
    IAgentFingerprintResolver GetFingerprintResolver(string agentConnectionId);
}