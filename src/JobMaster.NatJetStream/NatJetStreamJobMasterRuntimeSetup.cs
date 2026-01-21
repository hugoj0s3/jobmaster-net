using System;
using System.Collections.Generic;
using System.Linq;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc;
using JobMaster.Sdk.Abstractions.Ioc.Definitions;
using JobMaster.Sdk.Abstractions.Models;

namespace JobMaster.NatJetStream;

internal class NatJetStreamJobMasterRuntimeSetup : IJobMasterRuntimeSetup
{
    public Task<IList<string>> ValidateAsync(IServiceProvider mainServiceProvider)
    {
        var natAgentConfigs = JobMasterClusterConnectionConfig
            .GetAllConfigs()
            .SelectMany(x => x.GetAllAgentConnectionConfigs())
            .Where(a => a.RepositoryTypeId == NatJetStreamConstants.RepositoryTypeId)
            .ToList();
        
        var errors = new List<string>();
        
        foreach (var agentConfig in natAgentConfigs)
        {
            var clusterDefinition = BootstrapBlueprintDefinitions.Clusters.FirstOrDefault(c => c.ClusterId == agentConfig.ClusterId);
            if (clusterDefinition is null)
            {
                errors.Add($"Cluster {agentConfig.ClusterId} not found");
                continue;
            }
            
            var clusterTransientThreshold = clusterDefinition.TransientThreshold ?? new ClusterConfigurationModel(string.Empty).TransientThreshold;
            if (clusterTransientThreshold > NatJetStreamConstants.MaxThreshold)
            {
                errors.Add($@"
The transient threshold has to be defined and be less than {NatJetStreamConstants.MaxThreshold} for cluster {agentConfig.ClusterId}. 
NatJetStream requires a transient threshold of less than {NatJetStreamConstants.MaxThreshold}");
            }
        }
        
        return Task.FromResult<IList<string>>(errors);
    }

    public Task OnStartingAsync(IServiceProvider mainServiceProvider)
    {
        // Set default runtime throttle limit for agents using NatJetStream repository
        var natAgentConfigs = JobMasterClusterConnectionConfig
            .GetAllConfigs()
            .SelectMany(x => x.GetAllAgentConnectionConfigs())
            .Where(a => a.RepositoryTypeId == NatJetStreamConstants.RepositoryTypeId)
            .ToList();

        foreach (var agentConfig in natAgentConfigs)
        {
            if (!agentConfig.RuntimeDbOperationThrottleLimit.HasValue)
            {
                agentConfig.SetRuntimeDbOperationThrottleLimit(NatJetStreamConstants.DefaultDbOperationThrottleLimitForAgent);
            }
        }

        return Task.CompletedTask;
    }
}
