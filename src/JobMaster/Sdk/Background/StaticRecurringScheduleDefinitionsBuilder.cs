using System.Reflection;
using JobMaster.Abstractions;
using JobMaster.Abstractions.StaticRecurringSchedules;
using JobMaster.Sdk.Abstractions.Ioc.Definitions;
using JobMaster.Sdk.Abstractions.Jobs;
using JobMaster.Sdk.Reflection;
using JobMaster.Sdk.Utils;

namespace JobMaster.Sdk.Background;

/// <summary>
/// Builds every static recurring schedule definition (profiles + <see cref="RecurringScheduleAttribute"/> handlers) during
/// startup validation, so a broken profile or definition fails before anything starts. Every error is collected; the
/// startup saves the returned definitions only when there are none.
/// </summary>
internal static class StaticRecurringScheduleDefinitionsBuilder
{
    internal sealed class Result
    {
        public Result(IReadOnlyList<StaticRecurringScheduleDefinition> definitions, IList<string> errors)
        {
            Definitions = definitions;
            Errors = errors;
        }

        public IReadOnlyList<StaticRecurringScheduleDefinition> Definitions { get; }
        public IList<string> Errors { get; }
    }

    /// <summary>The profile types and <see cref="RecurringScheduleAttribute"/> handler types loaded in the AppDomain.</summary>
    public static Result Build(string defaultClusterId, IEnumerable<ClusterDefinition> clusters)
    {
        var attributeHandlerTypes = TypeDiscovery.AllTypes()
            .Where(t => !t.IsAbstract && t.GetCachedAttributes<RecurringScheduleAttribute>().Any());

        return Build(TypeDiscovery.StaticRecurringSchedulesProfileTypes(), attributeHandlerTypes, defaultClusterId, clusters);
    }

    public static Result Build(
        IEnumerable<Type> profileTypes,
        IEnumerable<Type> attributeHandlerTypes,
        string defaultClusterId,
        IEnumerable<ClusterDefinition> clusters)
    {
        var errors = new List<string>();
        var collections = new List<RecurringScheduleDefinitionCollection>();

        foreach (var pt in profileTypes)
        {
            var collection = TryBuildProfile(pt, defaultClusterId, errors);
            if (collection != null)
            {
                collections.Add(collection);
            }
        }

        // The lighter, profile-free alternative: an equivalent synthesized profile per handler.
        foreach (var handlerType in attributeHandlerTypes)
        {
            var collection = TryBuildAttributeHandler(handlerType, defaultClusterId, errors);
            if (collection != null)
            {
                collections.Add(collection);
            }
        }

        // A profile without its own ClusterId can span clusters (per-handler cluster id), so this goes by each
        // definition's resolved ClusterId, not the profile's.
        var definitions = collections.SelectMany(c => c.ToReadOnly()).ToList();

        // Each collection keeps its own ids unique; two profiles sharing a ProfileId would still collide, and the second
        // upsert would silently replace the first.
        foreach (var duplicate in definitions
                     .GroupBy(d => (Cluster: d.ClusterId.ToLowerInvariant(), Id: d.Id.ToLowerInvariant()))
                     .Where(g => g.Count() > 1))
        {
            errors.Add($"Duplicate static schedule Id '{duplicate.First().Id}' for cluster '{duplicate.First().ClusterId}' " +
                       "(two static recurring schedule profiles share a ProfileId?).");
        }

        var clusterList = clusters.ToList();
        foreach (var def in definitions)
        {
            var clusterDef = clusterList.SingleOrDefault(c =>
                string.Equals(c.ClusterId, def.ClusterId, StringComparison.OrdinalIgnoreCase));

            if (clusterDef?.DisabledPriorities.Any() != true) continue;

            // Same resolution as each run (Job.FromRecurringSchedule): explicit, else the handler's. The handler's
            // own cluster is checked at startup, but the schedule's cluster can differ.
            var priority = def.Priority ?? JobUtil.GetHandlerPriority(def.JobDefinitionId);
            if (priority.HasValue && clusterDef.DisabledPriorities.Contains(priority.Value))
            {
                errors.Add($"Static recurring schedule '{def.Id}' uses priority {priority.Value}, " +
                           $"which is disabled on cluster '{def.ClusterId}'.");
            }
        }

        return new Result(definitions, errors);
    }

    private static RecurringScheduleDefinitionCollection? TryBuildProfile(Type pt, string defaultClusterId, IList<string> errors)
    {
        try
        {
            var profileId = (string?)pt.GetProperty("ProfileId", BindingFlags.Public | BindingFlags.Static)?.GetValue(null);
            if (profileId == null)
            {
                errors.Add($"ProfileId missing on {pt.FullName}");
                return null;
            }

            var profileCluster = (string?)pt.GetProperty("ClusterId", BindingFlags.Public | BindingFlags.Static)?.GetValue(null);
            var workerLane = (string?)pt.GetProperty("WorkerLane", BindingFlags.Public | BindingFlags.Static)?.GetValue(null);

            // No profile ClusterId → each schedule resolves its handler's cluster, else default.
            var info = new StaticRecurringSchedulesProfileInfo(profileId, profileCluster, workerLane);
            if (!info.IsValid)
            {
                errors.Add($"Invalid ProfileId/ClusterId on profile {pt.FullName}");
                return null;
            }

            var configMethod = pt.GetMethod("Config", BindingFlags.Public | BindingFlags.Static);
            if (configMethod == null)
            {
                errors.Add($"Config(...) not found on profile {pt.FullName}");
                return null;
            }

            var collection = new RecurringScheduleDefinitionCollection(info, defaultClusterId);
            configMethod.Invoke(null, new object[] { collection });
            return collection;
        }
        catch (Exception ex)
        {
            errors.Add($"Static recurring schedule profile {pt.FullName} failed: {Unwrap(ex).Message}");
            return null;
        }
    }

    private static RecurringScheduleDefinitionCollection? TryBuildAttributeHandler(Type handlerType, string defaultClusterId, IList<string> errors)
    {
        if (!typeof(IJobMasterHandler).IsAssignableFrom(handlerType))
        {
            errors.Add($"{handlerType.FullName} has a RecurringScheduleAttribute but does not implement IJobMasterHandler.");
            return null;
        }

        try
        {
            var jobDefinitionId = handlerType.GetJobDefinitionId();
            var info = new StaticRecurringSchedulesProfileInfo($"attr:{jobDefinitionId}", clusterId: null, workerLane: null);
            if (!info.IsValid)
            {
                errors.Add($"Invalid synthesized ProfileId for {handlerType.FullName}.");
                return null;
            }

            var collection = new RecurringScheduleDefinitionCollection(info, defaultClusterId);
            foreach (var attr in handlerType.GetCachedAttributes<RecurringScheduleAttribute>())
            {
                collection.Add(handlerType, attr.ExpressionTypeId, attr.Expression);
            }

            return collection;
        }
        catch (Exception ex)
        {
            errors.Add($"RecurringScheduleAttribute on {handlerType.FullName} failed: {Unwrap(ex).Message}");
            return null;
        }
    }

    // Config is invoked by reflection, so its own exception arrives wrapped.
    private static Exception Unwrap(Exception ex) =>
        ex is TargetInvocationException { InnerException: { } inner } ? inner : ex;
}
