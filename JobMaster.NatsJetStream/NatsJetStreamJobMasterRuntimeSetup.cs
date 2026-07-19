using System;
using System.Collections.Generic;
using System.Linq;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc;
using JobMaster.Sdk.Abstractions.Ioc.Definitions;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Utils;

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
                var docUrl = JobMasterDocUrls.Page(JobMasterDocUrls.Pages.NatsProvider, "transientthreshold-and-nats-capacity");
                errors.Add(
                    $"Cluster {agentConfig.ClusterId}: TransientThreshold ({clusterTransientThreshold}) must be less than or equal to {NatsJetStreamConstants.MaxThreshold} when using NatsJetStream. See {docUrl}");
            }
        }
        
        return Task.FromResult<IList<string>>(errors);
    }

    public Task OnBeforeStartAsync(IServiceProvider mainServiceProvider)
    {
        // Set default runtime throttle limit for agents using NatsJetStream repository
        var natAgentConfigs = JobMasterClusterConnectionConfig
            .GetAllConfigs()
            .SelectMany(x => x.GetAllAgentConnectionConfigs())
            .Where(a => a.RepositoryTypeId == NatsJetStreamConstants.RepositoryTypeId)
            .ToList();

        foreach (var agentConfig in natAgentConfigs)
        {
            if (!agentConfig.RuntimeDbOperationLimit.HasValue)
            {
                agentConfig.SetRuntimeDbOperationLimit(NatsJetStreamConstants.DefaultDbOperationThrottleLimitForAgent);
            }
        }

        return Task.CompletedTask;
    }
}
