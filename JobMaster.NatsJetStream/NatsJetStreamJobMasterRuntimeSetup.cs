using System;
using System.Collections.Generic;
using System.Linq;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc;
using JobMaster.Sdk.Abstractions.Ioc.Definitions;
using JobMaster.Sdk.Abstractions.Models;

namespace JobMaster.NatsJetStream;

internal class NatsJetStreamJobMasterRuntimeSetup : IJobMasterRuntimeSetup
{
    public Task<IList<string>> ValidateAsync(IServiceProvider mainServiceProvider)
    {
        var natAgentConfigs = JobMasterClusterConnectionConfig
            .GetAllConfigs()
            .SelectMany(x => x.GetAllAgentConnectionConfigs())
            .Where(a => a.RepositoryTypeId == NatsJetStreamConstants.RepositoryTypeId)
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
            if (clusterTransientThreshold > NatsJetStreamConstants.MaxThreshold)
            {
                errors.Add($@"
The transient threshold has to be defined and be less than {NatsJetStreamConstants.MaxThreshold} for cluster {agentConfig.ClusterId}. 
NatsJetStream requires a transient threshold of less than {NatsJetStreamConstants.MaxThreshold}");
            }
        }
        
        return Task.FromResult<IList<string>>(errors);
    }

    public Task OnStartingAsync(IServiceProvider mainServiceProvider)
    {
        // Set default runtime throttle limit for agents using NatsJetStream repository
        var natAgentConfigs = JobMasterClusterConnectionConfig
            .GetAllConfigs()
            .SelectMany(x => x.GetAllAgentConnectionConfigs())
            .Where(a => a.RepositoryTypeId == NatsJetStreamConstants.RepositoryTypeId)
            .ToList();

        foreach (var agentConfig in natAgentConfigs)
        {
            if (!agentConfig.RuntimeDbOperationThrottleLimit.HasValue)
            {
                agentConfig.SetRuntimeDbOperationThrottleLimit(NatsJetStreamConstants.DefaultDbOperationThrottleLimitForAgent);
            }
        }

        return Task.CompletedTask;
    }
}
