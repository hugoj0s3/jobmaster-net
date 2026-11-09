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
        // Only restrict TransientThreshold when NATS JetStream plays the Execution role
        // (standalone NATS connection or hybrid connection with NATS on Execution).
        // When NATS is used only for Transport, jobs scheduled beyond 5 minutes are held on
        // the master and dispatched to the non-NATS Execution provider later.
        var natExecutionAgentConfigs = JobMasterClusterConnectionConfig
            .GetAllConfigs()
            .SelectMany(x => x.GetAllAgentConnectionConfigs())
            .Where(a => a.ExecutionConfig.RepositoryTypeId == NatsJetStreamConstants.RepositoryTypeId)
            .ToList();
        
        var errors = new List<string>();
        
        foreach (var agentConfig in natExecutionAgentConfigs)
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
            schedulingThrottlerSettingsTemplate: new OperationThrottlerSettingsTemplate(500, 250));

        return Task.CompletedTask;
    }
}
