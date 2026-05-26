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
    private IDictionary<string, IAgentFingerprintResolver> fingerprintResolversByAgentConnectionId
        = new Dictionary<string, IAgentFingerprintResolver>();
    private IJobMasterClusterAwareComponentFactory AwareComponentFactory => JobMasterClusterAwareComponentFactories.GetFactory(this.ClusterConnConfig.ClusterId);

    public AgentComponentFactory(JobMasterClusterConnectionConfig clusterConnectionConfig) : base(clusterConnectionConfig)
    {
    }
    
    public IAgentJobsDispatcherRepository GetRepository(AgentConnectionId agentConnectionId)
    {
        if (!agentConnectionId.IsValid())
        {
            throw new Exception($"Agent connection {agentConnectionId} is not active");
        }
        
        return GetRepository(agentConnectionId.IdValue);
    }

    public IAgentFingerprintResolver GetFingerprintResolver(AgentConnectionId agentConnectionId)
    {
        var connectionId = agentConnectionId.IdValue;
        return GetFingerprintResolver(connectionId);
    }

    public IAgentFingerprintResolver GetFingerprintResolver(string connectionId)
    {
        if (fingerprintResolversByAgentConnectionId.TryGetValue(connectionId, out var fingerprintResolver))
        {
            return fingerprintResolver;
        }
        
        var config = GetAgentConnectionConfig(connectionId);
        
      
        if (config == null)
        {
            throw new Exception($"Connection string for agent {connectionId} not found");
        }
        
        var repo = 
            AwareComponentFactory.GetFingerprintResolver(config.RepositoryTypeId);
        
        repo.Initialize(config);
        
        fingerprintResolversByAgentConnectionId[connectionId] = repo;
        
        return fingerprintResolversByAgentConnectionId[connectionId];
    }

    public IAgentJobsDispatcherRepository GetRepository(string agentConnectionId)
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
            .TryGet(this.ClusterConnConfig.ClusterId, includeNotReady: true)?
            .TryGetAgentConnectionConfig(agentConnectionId);
        
        return agentCnnConfig;
    }
}