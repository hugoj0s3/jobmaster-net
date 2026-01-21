using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.Sdk.Ioc;
using JobMaster.Sdk.Ioc.Markups;

namespace JobMaster.Sdk.Repositories;

public class AgentJobsDispatcherRepositoryFactory : JobMasterClusterAwareComponent, IAgentJobsDispatcherRepositoryFactory
{
    private IDictionary<string, IAgentJobsDispatcherRepository> repositoriesByAgentConnectionId = new Dictionary<string, IAgentJobsDispatcherRepository>();
    private IJobMasterClusterAwareComponentFactory AwareComponentFactory => JobMasterClusterAwareComponentFactories.GetFactory(this.ClusterConnConfig.ClusterId);

    public AgentJobsDispatcherRepositoryFactory(JobMasterClusterConnectionConfig clusterConnectionConfig) : base(clusterConnectionConfig)
    {
    }
    
    public IAgentJobsDispatcherRepository GetRepository(AgentConnectionId agentConnectionId)
    {
        if (!agentConnectionId.IsActive())
        {
            throw new Exception($"Agent connection {agentConnectionId} is not active");
        }
        
        return GetRepository((string)agentConnectionId.IdValue);
    }
    
    private IAgentJobsDispatcherRepository GetRepository(string agentConnectionId)
    {
         if (repositoriesByAgentConnectionId.TryGetValue(agentConnectionId, out var repository))
         {
             return repository;
         }
         
         var agentCnnConfig = JobMasterClusterConnectionConfig.TryGet(this.ClusterConnConfig.ClusterId)?.TryGetAgentConnectionConfig(agentConnectionId);
         if (agentCnnConfig == null)
         {
             throw new Exception($"Connection string for agent {agentConnectionId} not found");
         }
         
         repositoriesByAgentConnectionId[agentConnectionId] = AwareComponentFactory.GetRepositoryDispatcher(agentCnnConfig.RepositoryTypeId);
         repositoriesByAgentConnectionId[agentConnectionId].Initialize(agentCnnConfig);
         
         return repositoriesByAgentConnectionId[agentConnectionId];
    }
}