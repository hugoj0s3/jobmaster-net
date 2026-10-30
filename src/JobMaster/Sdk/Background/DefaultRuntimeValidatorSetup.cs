using System.Reflection;
using JobMaster.Abstractions;
using JobMaster.Abstractions.Models;
using JobMaster.Abstractions.Models.Attributes;
using JobMaster.Abstractions.StaticRecurringSchedules;
using JobMaster.Sdk.Abstractions.Config;
using JobMaster.Sdk.Abstractions.Ioc;
using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Sdk.Abstractions.Ioc.Definitions;
using JobMaster.Sdk.Utils;

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

        // IStaticJobDefinitionConfig.Config is compile-time enforced via `static abstract` on net8.0, but
        // not on netstandard2.0 (no static abstract interface members there) -- this is the only thing
        // that catches a missing Config on that target framework. Checked (and returned on, if violated)
        // before handlerTypes below, which resolves JobDefinitionId via JobMasterDefinitionIdAttribute ->
        // JobDefinitionConfigAttribute.GetConfig and would otherwise throw uncaught partway through
        // discovery instead of reporting a clean, aggregated error like every other check here.
        var staticJobDefinitionConfigTypes = typeof(IStaticJobDefinitionConfig).GetCachedConcreteTypesOf().ToList();
        var staticJobDefinitionConfigTypesMissingConfig = staticJobDefinitionConfigTypes
            .Where(t => !JobDefinitionConfigAttribute.TryGetConfig(t, out _))
            .ToList();

        if (staticJobDefinitionConfigTypesMissingConfig.Any())
        {
            result.Add("The following IStaticJobDefinitionConfig implementations do not declare a valid " +
                       "`public static JobDefinitionConfig Config { get; }` member: " +
                       $"{string.Join(", ", staticJobDefinitionConfigTypesMissingConfig.Select(t => t.FullName))}");
            return Task.FromResult<IList<string>>(result);
        }

        var handlerTypes = typeof(IJobMasterHandler).GetCachedConcreteTypesOf()
            .Select(x => new
            {
                Type = x,
                JobDefinitionId = x.GetJobDefinitionId()
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

        var multipleDefinitionAttributesError = ValidateSingleDefinitionAttribute(handlerTypes.Select(x => x.Type));
        if (multipleDefinitionAttributesError != null)
        {
            result.Add(multipleDefinitionAttributesError);
        }

        var handlerTypesMixingDefinitionAttributeFamilies = handlerTypes
            .Select(x => x.Type)
            .Where(t => t.GetCachedAttributes<JobDefinitionConfigAttribute>().Any() &&
                        (t.GetCachedAttribute<JobMasterDefinitionIdAttribute>() != null ||
                         t.GetCachedAttribute<JobMasterTimeoutAttribute>() != null ||
                         t.GetCachedAttribute<JobMasterPriorityAttribute>() != null ||
                         t.GetCachedAttribute<JobMasterWorkerLaneAttribute>() != null ||
                         t.GetCachedAttribute<JobMasterMaxNumberOfRetriesAttribute>() != null ||
                         t.GetCachedAttribute<JobMasterClusterIdAttribute>() != null))
            .ToList();

        if (handlerTypesMixingDefinitionAttributeFamilies.Any())
        {
            result.Add("Job handlers must not combine a JobDefinitionConfigAttribute with individual classic " +
                       "attributes (JobMasterDefinitionId/JobMasterTimeout/JobMasterPriority/JobMasterWorkerLane/" +
                       $"JobMasterMaxNumberOfRetries/JobMasterClusterId) — pick one: {string.Join(", ", handlerTypesMixingDefinitionAttributeFamilies.Select(t => t.FullName))}");
        }

        // Missing Config was already returned on above, so GetConfig can't throw here.
        var unknownClusterIdError = ValidateClusterIds(
            handlerTypes.Select(x => x.Type),
            staticJobDefinitionConfigTypes,
            BootstrapBlueprintDefinitions.Clusters.Select(c => c.ClusterId));
        if (unknownClusterIdError != null)
        {
            result.Add(unknownClusterIdError);
        }

        var disabledPriorityError = ValidateDisabledPriorities(
            handlerTypes.Select(x => x.Type),
            staticJobDefinitionConfigTypes,
            BootstrapBlueprintDefinitions.Clusters,
            JobMasterClusterConnectionConfig.Default?.ClusterId);
        if (disabledPriorityError != null)
        {
            result.Add(disabledPriorityError);
        }

        // Same discovery as JobMasterRuntime.BootstrapStaticRecurringSchedules — without this, an unknown profile
        // ClusterId only surfaces there as a KeyNotFoundException, after the workers have already started.
        var staticProfileTypes = TypeDiscovery.StaticRecurringSchedulesProfileTypes();

        var unknownProfileClusterIdError = ValidateStaticProfileClusterIds(
            staticProfileTypes,
            BootstrapBlueprintDefinitions.Clusters.Select(c => c.ClusterId));
        if (unknownProfileClusterIdError != null)
        {
            result.Add(unknownProfileClusterIdError);
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

    /// <summary>
    /// Returns an error when a handler applies more than one <see cref="JobDefinitionConfigAttribute"/> (e.g. two
    /// different subclasses) -- its identity and config would be ambiguous; <c>null</c> when all are fine. Enforced
    /// here, not in the runtime lookups, which just take the first match so one bad handler can't break other jobs.
    /// </summary>
    internal static string? ValidateSingleDefinitionAttribute(IEnumerable<Type> handlerTypes)
    {
        var invalid = handlerTypes
            .Where(t => t.GetCachedAttributes<JobDefinitionConfigAttribute>().Count() > 1)
            .ToList();

        if (!invalid.Any())
        {
            return null;
        }

        return "Job handlers must not apply more than one JobDefinitionConfigAttribute: " +
               $"{string.Join(", ", invalid.Select(t => t.FullName))}";
    }

    /// <summary>
    /// Returns an error when a handler's own cluster id (applied <see cref="JobDefinitionConfig.ClusterId"/> or
    /// <see cref="JobMasterClusterIdAttribute"/>) or an <see cref="IStaticJobDefinitionConfig"/>'s
    /// <see cref="JobDefinitionConfig.ClusterId"/> isn't a configured cluster, so it fails at startup instead of at
    /// the first schedule call; <c>null</c> when all are fine.
    /// </summary>
    internal static string? ValidateClusterIds(
        IEnumerable<Type> handlerTypes,
        IEnumerable<Type> staticJobDefinitionConfigTypes,
        IEnumerable<string?> configuredClusterIds)
    {
        var configuredClusterIdSet = ToClusterIdSet(configuredClusterIds);

        var declared = handlerTypes
            .Select(t => (Type: t, ClusterId: JobUtil.GetClusterId(t, clusterId: null)))
            .Concat(staticJobDefinitionConfigTypes
                .Select(t => (Type: t, ClusterId: JobDefinitionConfigAttribute.GetConfig(t).ClusterId)));

        var unknown = declared
            .Where(x => x.ClusterId != null && !configuredClusterIdSet.Contains(x.ClusterId))
            .ToList();

        if (!unknown.Any())
        {
            return null;
        }

        return "Job handlers / IStaticJobDefinitionConfig implementations declare a cluster id (JobMasterClusterId / " +
               "JobDefinitionConfig.ClusterId) that is not configured: " +
               $"{string.Join(", ", unknown.Select(x => $"{x.Type.FullName} ('{x.ClusterId}')"))}";
    }

    /// <summary>
    /// Returns an error when a handler's resolved priority (<see cref="JobDefinitionConfig.Priority"/> or inherited
    /// <see cref="JobMasterPriorityAttribute"/>, else Medium) is disabled on the handler's own cluster (its cluster id,
    /// else <paramref name="defaultClusterId"/>), or an <see cref="IStaticJobDefinitionConfig"/>'s priority is disabled
    /// on its <see cref="JobDefinitionConfig.ClusterId"/>; <c>null</c> when all are fine. Only the item's own cluster
    /// counts: a priority disabled on another cluster doesn't matter. Explicit <c>clusterId:</c> routing is checked
    /// at the scheduling call; unknown cluster ids are reported by <see cref="ValidateClusterIds"/>.
    /// </summary>
    internal static string? ValidateDisabledPriorities(
        IEnumerable<Type> handlerTypes,
        IEnumerable<Type> staticJobDefinitionConfigTypes,
        IEnumerable<ClusterDefinition> clusters,
        string? defaultClusterId)
    {
        var clusterList = clusters.ToList();
        if (!clusterList.Any(c => c.DisabledPriorities.Any()))
        {
            return null;
        }

        var declared = handlerTypes
            .Select(t => (Type: t,
                ClusterId: JobUtil.GetClusterId(t, clusterId: null) ?? defaultClusterId,
                Priority: JobUtil.GetJobMasterPriority(t, priority: null)))
            .Concat(staticJobDefinitionConfigTypes
                .Select(t => (Type: t, Config: JobDefinitionConfigAttribute.GetConfig(t)))
                .Select(x => (x.Type,
                    ClusterId: x.Config.ClusterId ?? defaultClusterId,
                    Priority: x.Config.Priority ?? JobMasterPriority.Medium)));

        var invalid = declared
            .Select(x => (x.Type, x.ClusterId, x.Priority, Cluster: clusterList.FirstOrDefault(c =>
                string.Equals(c.ClusterId, x.ClusterId, StringComparison.OrdinalIgnoreCase))))
            .Where(x => x.Cluster != null && x.Cluster.DisabledPriorities.Contains(x.Priority))
            .ToList();

        if (!invalid.Any())
        {
            return null;
        }

        return "Job handlers / IStaticJobDefinitionConfig implementations use a priority that is disabled on their " +
               "cluster (DisablePriority): " +
               $"{string.Join(", ", invalid.Select(x => $"{x.Type.FullName} ({x.Priority} on '{x.Cluster!.ClusterId}')"))}";
    }

    /// <summary>
    /// Returns an error when an <see cref="IStaticRecurringSchedulesProfile"/>'s static <c>ClusterId</c> is set but
    /// isn't a configured cluster; <c>null</c> when all are fine. A blank/unset <c>ClusterId</c> is valid (each
    /// schedule then falls back to its handler's cluster, which <see cref="ValidateClusterIds"/> covers).
    /// </summary>
    internal static string? ValidateStaticProfileClusterIds(IEnumerable<Type> profileTypes, IEnumerable<string?> configuredClusterIds)
    {
        var configuredClusterIdSet = ToClusterIdSet(configuredClusterIds);

        var unknown = profileTypes
            .Select(t => (Type: t, ClusterId: (string?)t.GetProperty("ClusterId", BindingFlags.Public | BindingFlags.Static)?.GetValue(null)))
            .Where(x => !string.IsNullOrWhiteSpace(x.ClusterId) && !configuredClusterIdSet.Contains(x.ClusterId!))
            .ToList();

        if (!unknown.Any())
        {
            return null;
        }

        return "Static recurring schedule profiles declare a ClusterId that is not configured: " +
               $"{string.Join(", ", unknown.Select(x => $"{x.Type.FullName} ('{x.ClusterId}')"))}";
    }

    private static HashSet<string> ToClusterIdSet(IEnumerable<string?> configuredClusterIds)
    {
        var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var clusterId in configuredClusterIds)
        {
            if (!string.IsNullOrEmpty(clusterId))
            {
                result.Add(clusterId!);
            }
        }

        return result;
    }

    public Task OnBeforeStartAsync(IServiceProvider mainServiceProvider)
    {
       return Task.CompletedTask;
    }
}