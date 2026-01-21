using System.ComponentModel;
using JobMaster.Sdk.Abstractions.Ioc.Markups;
using JobMaster.Sdk.Abstractions.Models.Agents;

namespace JobMaster.Sdk.Abstractions.Repositories.Agent;

[EditorBrowsable(EditorBrowsableState.Never)]
public interface IAgentJobsDispatcherRepositoryFactory : IJobMasterClusterAwareComponent
{
    IAgentJobsDispatcherRepository GetRepository(AgentConnectionId agentConnectionId);
}