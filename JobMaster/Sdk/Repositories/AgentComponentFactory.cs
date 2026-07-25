using System.Collections.Concurrent;
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
    private readonly ConcurrentDictionary<string, IAgentJobsDispatcherRepository> repositoriesByAgentConnectionId = new();
    private readonly ConcurrentDictionary<string, IAgentFingerprintResolver> fingerprintResolversByAgentConnectionId = new();
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

        return fingerprintResolversByAgentConnectionId.GetOrAdd(connectionId, _ =>
        {
            var repo = AwareComponentFactory.GetFingerprintResolver(config.RepositoryTypeId);
            repo.Initialize(config);
            return repo;
        });
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

         return repositoriesByAgentConnectionId.GetOrAdd(agentConnectionId, _ =>
         {
             var repo = AwareComponentFactory.GetRepositoryDispatcher(agentCnnConfig.RepositoryTypeId);
             repo.Initialize(agentCnnConfig);
             return repo;
         });
    }

    private JobMasterAgentConnectionConfig? GetAgentConnectionConfig(string agentConnectionId)
    {
        var agentCnnConfig = JobMasterClusterConnectionConfig
            .TryGet(this.ClusterConnConfig.ClusterId, includeNotReady: true)?
            .TryGetAgentConnectionConfig(agentConnectionId);
        
        return agentCnnConfig;
    }
}