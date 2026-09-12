using System.Collections.Concurrent;
using System.Reflection;

namespace JobMaster.Abstractions.Models.Attributes;

/// <summary>
/// Assigns a stable, human-readable ID to a job handler class.
/// The runtime uses this ID to locate the correct handler when a job is dispatched.
/// If omitted, the handler's full type name (<see cref="Type.FullName"/>) is used as the ID.
/// Prefer setting an explicit ID to avoid breakage if the class is renamed or moved to different namespace.
/// </summary>
[AttributeUsage(AttributeTargets.Class)]
public class JobMasterDefinitionIdAttribute : Attribute
{
    /// <summary>Initializes the attribute with the given definition ID.</summary>
    /// <param name="jobDefinitionId">The stable, human-readable ID to assign to the handler.</param>
    public JobMasterDefinitionIdAttribute(string jobDefinitionId)
    {
        JobDefinitionId = jobDefinitionId;
    }

    /// <summary>The stable, human-readable ID assigned to the job handler.</summary>
    public string JobDefinitionId { get; }

    private static readonly ConcurrentDictionary<string, Type> JobDefinitionIdMap = new();

    /// <summary>
    /// Resolves the <see cref="IJobMasterHandler"/> implementation type for the given <paramref name="jobdefinitionId"/>.
    /// Returns <c>null</c> if no matching type is found.
    /// </summary>
    public static Type? GetJobHandlerTypeFromId(string jobdefinitionId)
    {
        if (JobDefinitionIdMap.TryGetValue(jobdefinitionId, out var result))
        {
            return result;
        }

        var type = typeof(IJobMasterHandler);
        var types = AppDomain.CurrentDomain.GetAssemblies()
            .SelectMany(s => s.GetTypes())
            .Where(p => type.IsAssignableFrom(p))
            .Where(p => p.IsAbstract == false && p is { IsInterface: false, IsClass: true })
            .ToList();

        var byConfigAttribute = types.FirstOrDefault(x =>
            JobDefinitionConfigAttribute.TryGetAppliedConfig(x)?.JobDefinitionId == jobdefinitionId);
        if (byConfigAttribute is not null)
        {
            JobDefinitionIdMap.TryAdd(jobdefinitionId, byConfigAttribute);
            return byConfigAttribute;
        }
        
        var byDefinitionIdAttribute = types.FirstOrDefault(x =>
            x.GetCustomAttributes<JobMasterDefinitionIdAttribute>().FirstOrDefault()?.JobDefinitionId == jobdefinitionId);
        if (byDefinitionIdAttribute is not null)
        {
            JobDefinitionIdMap.TryAdd(jobdefinitionId, byDefinitionIdAttribute);
            return byDefinitionIdAttribute;
        }
        
        var byFullName = types.FirstOrDefault(x => x.FullName == jobdefinitionId);
        if (byFullName is not null)
        {
            JobDefinitionIdMap.TryAdd(jobdefinitionId, byFullName);
            return byFullName;
        }
        
        return null;
    }

    /// <summary>
    /// Returns the definition ID for <paramref name="type"/>: the value from a
    /// <see cref="JobDefinitionConfigAttribute"/> if present, otherwise from
    /// <see cref="JobMasterDefinitionIdAttribute"/> if present, otherwise <see cref="Type.FullName"/>.
    /// </summary>
    public static string GetJobDefinitionId(Type type)
    {
        return JobDefinitionConfigAttribute.TryGetAppliedConfig(type)?.JobDefinitionId
               ?? type.GetCustomAttribute<JobMasterDefinitionIdAttribute>()?.JobDefinitionId
               ?? type.FullName!;
    }
}
