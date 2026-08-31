using System.Reflection;
using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.Models.Attributes;
using JobMaster.Sdk.Abstractions.Ioc;
using JobMaster.Sdk.Abstractions.Ioc.Definitions;

namespace JobMaster.Sdk.Background;

internal class DefaultRuntimeValidatorSetup : IJobMasterRuntimeSetup
{
    public Task<IList<string>> ValidateAsync(IServiceProvider mainServiceProvider)
    {
        var result = new List<string>();

        var clustersWithTransientThresholdGreaterThan24Hours = BootstrapBlueprintDefinitions.Clusters
            .Where(x => x.TransientThreshold > TimeSpan.FromHours(24))
            .ToList();
        foreach (var cluster in clustersWithTransientThresholdGreaterThan24Hours)
        {
            result.Add($"{cluster.ClusterId} is configured with a TransientThreshold greater than 24 hours. This is not allowed.");
        }

        var assemblies = AppDomain.CurrentDomain.GetAssemblies()
            .Where(a => !a.IsDynamic && !string.IsNullOrEmpty(a.Location))
            .ToList();

        // IStaticJobDefinitionConfig.Config is compile-time enforced via `static abstract` on net8.0, but
        // not on netstandard2.0 (no static abstract interface members there) -- this is the only thing
        // that catches a missing Config on that target framework. Checked (and returned on, if violated)
        // before handlerTypes below, which resolves JobDefinitionId via JobMasterDefinitionIdAttribute ->
        // JobDefinitionConfigAttribute.GetConfig and would otherwise throw uncaught partway through
        // discovery instead of reporting a clean, aggregated error like every other check here.
        var staticJobDefinitionConfigTypesMissingConfig = assemblies
            .SelectMany(a => a.GetTypes())
            .Where(t => typeof(IStaticJobDefinitionConfig).IsAssignableFrom(t) &&
                        !t.IsInterface &&
                        !t.IsAbstract &&
                        !JobDefinitionConfigAttribute.TryGetConfig(t, out _))
            .ToList();

        if (staticJobDefinitionConfigTypesMissingConfig.Any())
        {
            result.Add("The following IStaticJobDefinitionConfig implementations do not declare a valid " +
                       "`public static JobDefinitionConfig Config { get; }` member: " +
                       $"{string.Join(", ", staticJobDefinitionConfigTypesMissingConfig.Select(t => t.FullName))}");
            return Task.FromResult<IList<string>>(result);
        }

        var handlerTypes = assemblies
            .SelectMany(a => a.GetTypes())
            .Where(t => typeof(IJobMasterHandler).IsAssignableFrom(t) &&
                        !t.IsInterface &&
                        !t.IsAbstract)
            .Select(x => new
            {
                Type = x,
                JobDefinitionId = JobMasterDefinitionIdAttribute.GetJobDefinitionId(x)
            })
            .ToList();

       var handlerTypesWithDuplicateJobDefinitionIds = handlerTypes.GroupBy(x => x.JobDefinitionId)
            .Where(g => g.Count() > 1)
            .Select(g => g.Key)
            .ToList();

       if (handlerTypesWithDuplicateJobDefinitionIds.Any())
       {
           result.Add($"Multiple job handlers found with the same JobDefinitionId: {string.Join(", ", handlerTypesWithDuplicateJobDefinitionIds)}");
       }

        var handlerTypesMixingDefinitionAttributeFamilies = handlerTypes
            .Select(x => x.Type)
            .Where(t => t.GetCustomAttribute<JobDefinitionConfigAttribute>() != null &&
                        (t.GetCustomAttribute<JobMasterDefinitionIdAttribute>() != null ||
                         t.GetCustomAttribute<JobMasterTimeoutAttribute>() != null ||
                         t.GetCustomAttribute<JobMasterPriorityAttribute>() != null ||
                         t.GetCustomAttribute<JobMasterWorkerLaneAttribute>() != null ||
                         t.GetCustomAttribute<JobMasterMaxNumberOfRetriesAttribute>() != null))
            .ToList();

        if (handlerTypesMixingDefinitionAttributeFamilies.Any())
        {
            result.Add("Job handlers must not combine a JobDefinitionConfigAttribute with individual classic " +
                       "attributes (JobMasterDefinitionId/JobMasterTimeout/JobMasterPriority/JobMasterWorkerLane/" +
                       $"JobMasterMaxNumberOfRetries) — pick one: {string.Join(", ", handlerTypesMixingDefinitionAttributeFamilies.Select(t => t.FullName))}");
        }

        // Coordinator workers deliberately have no AgentConnectionName (see ChangeLog.md 0.0.10-alpha:
        // "Coordinator workers no longer take an agent connection — and now must not have one") —
        // enforced separately elsewhere, so they're exempt from this check.
        var workersWithoutAgentConnectionName = BootstrapBlueprintDefinitions.Clusters
            .SelectMany(x => x.Workers)
            .Where(x => x.Mode != AgentWorkerMode.Coordinator && string.IsNullOrEmpty(x.AgentConnectionName))
            .ToList();
        if (workersWithoutAgentConnectionName.Any())
        {
            result.Add($"Workers without AgentConnectionName: {string.Join(", ", workersWithoutAgentConnectionName.Select(x => x.WorkerName))}");
        }

        return Task.FromResult<IList<string>>(result);
    }

    public Task OnBeforeStartAsync(IServiceProvider mainServiceProvider)
    {
       return Task.CompletedTask;
    }
}