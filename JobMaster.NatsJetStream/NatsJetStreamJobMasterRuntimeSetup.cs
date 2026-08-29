using System;
using System.Collections.Generic;
using System.Linq;
using JobMaster.Sdk.Abstractions;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc;
using JobMaster.Sdk.Abstractions.Ioc.Definitions;
using JobMaster.Sdk.Abstractions.Models;
using JobMaster.Sdk.Abstractions.Repositories;
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
        // NatsJetStream is never used as a cluster/master repository type, only as an agent
        // transport -- so only RegisterForAgent is called here, no RegisterForMaster.
        OperationThrottlerSettingsTemplateFactory.RegisterForAgent(
            NatsJetStreamConstants.RepositoryTypeId,
            maxBatchSize: 50,
            internalThrottlerSettingsTemplate: new OperationThrottlerSettingsTemplate(250, 1000),
            schedulingThrottlerSettingsTemplate: new OperationThrottlerSettingsTemplate(100, 125));

        return Task.CompletedTask;
    }
}
