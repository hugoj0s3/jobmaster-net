using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc;
using JobMaster.Sdk.Abstractions.Models.Agents;
using JobMaster.Sdk.Abstractions.Repositories.Agent;
using JobMaster.Sdk.Ioc;
using JobMaster.Sdk.Ioc.Markups;

namespace JobMaster.Sdk.Repositories;

internal class AgentComponentFactory : JobMasterClusterAwareComponent, IAgentComponentFactory
{
    private IDictionary<string, IAgentJobsDispatcherRepository> repositoriesByAgentConnectionId 
        = new Dictionary<string, IAgentJobsDispatcherRepository>();
    private IDictionary<string, IAgentFootprintResolver> footprintResolversByAgentConnectionId 
        = new Dictionary<string, IAgentFootprintResolver>();
    private IJobMasterClusterAwareComponentFactory AwareComponentFactory => JobMasterClusterAwareComponentFactories.GetFactory(this.ClusterConnConfig.ClusterId);

    public AgentComponentFactory(JobMasterClusterConnectionConfig clusterConnectionConfig) : base(clusterConnectionConfig)
    {
    }
    
    public IAgentJobsDispatcherRepository GetRepository(AgentConnectionId agentConnectionId)
    {
        if (!agentConnectionId.IsActive())
        {
            throw new Exception($"Agent connection {agentConnectionId} is not active");
        }
        
        return GetRepository(agentConnectionId.IdValue);
    }

    public IAgentFootprintResolver GetFootprintResolver(AgentConnectionId agentConnectionId)
    {
        var connectionId = agentConnectionId.IdValue;
        if (footprintResolversByAgentConnectionId.TryGetValue(connectionId, out var footprintResolver))
        {
            return footprintResolver;
        }
        
        var repo = 
            AwareComponentFactory.GetFootprintResolver(connectionId);
        
        var config = GetAgentConnectionConfig(connectionId);
        if (config == null)
        {
            throw new Exception($"Connection string for agent {connectionId} not found");
        }
        
        repo.Initialize(config);
        
        footprintResolversByAgentConnectionId[connectionId] = repo;
        
        return footprintResolversByAgentConnectionId[connectionId];
    }

    private IAgentJobsDispatcherRepository GetRepository(string agentConnectionId)
    {
         if (repositoriesByAgentConnectionId.TryGetValue(agentConnectionId, out var repository))
         {
             return repository;
         }
         
         var agentCnnConfig = GetAgentConnectionConfig(agentConnectionId);
         if (agentCnnConfig == null)
         {
             throw new Exception($"Connection string for agent {agentConnectionId} not found");
         }
         
         repositoriesByAgentConnectionId[agentConnectionId] = AwareComponentFactory.GetRepositoryDispatcher(agentCnnConfig.RepositoryTypeId);
         repositoriesByAgentConnectionId[agentConnectionId].Initialize(agentCnnConfig);
         
         return repositoriesByAgentConnectionId[agentConnectionId];
    }

    private JobMasterAgentConnectionConfig? GetAgentConnectionConfig(string agentConnectionId)
    {
        var agentCnnConfig = JobMasterClusterConnectionConfig
            .TryGet(this.ClusterConnConfig.ClusterId)?
            .TryGetAgentConnectionConfig(agentConnectionId);
        
        return agentCnnConfig;
    }
}