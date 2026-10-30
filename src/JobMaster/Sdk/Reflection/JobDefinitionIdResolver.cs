using System.Collections.Concurrent;
using JobMaster.Abstractions;
using JobMaster.Abstractions.Models.Attributes;
using JobMaster.Sdk.Utils;

namespace JobMaster.Sdk.Reflection;

/// <summary>Maps job definition ids to handler types and back.</summary>
internal static class JobDefinitionIdResolver
{
    private static readonly ConcurrentDictionary<string, Type> JobDefinitionIdMap = new();

    internal const string AssemblyDiscoveryHint =
        "If the handler is in a library this application never references in code, add it with " +
        "services.JobMasterAssemblyDiscovery().AddAssemblyContaining<THandler>().";

    /// <summary>
    /// Resolves the <see cref="IJobMasterHandler"/> implementation type for the given <paramref name="jobDefinitionId"/>.
    /// Returns <c>null</c> if no matching type is found.
    /// </summary>
    public static Type? GetJobHandlerTypeFromId(string jobDefinitionId)
    {
        if (JobDefinitionIdMap.TryGetValue(jobDefinitionId, out var result))
        {
            return result;
        }

        // Miss: map every handler (all ids at once). The handler types are cached by TypeDiscovery, so this is cheap.
        foreach (var handlerType in typeof(IJobMasterHandler).GetCachedConcreteTypesOf())
        {
            // Duplicate ids are a startup validation error; the first one found wins here.
            JobDefinitionIdMap.TryAdd(handlerType.GetJobDefinitionId(), handlerType);
        }

        return JobDefinitionIdMap.TryGetValue(jobDefinitionId, out result) ? result : null;
    }

    /// <summary>
    /// Resolves the handler for <paramref name="jobDefinitionId"/> from <paramref name="serviceProvider"/>.
    /// Throws when no handler type has that id or the type isn't registered in the DI container.
    /// </summary>
    public static IJobMasterHandler GetJobHandler(this IServiceProvider serviceProvider, string jobDefinitionId)
    {
        var handlerType = GetJobHandlerTypeFromId(jobDefinitionId);
        if (handlerType is null)
        {
            throw new InvalidOperationException($"Job handler type not found for job definition id: {jobDefinitionId}. {AssemblyDiscoveryHint}");
        }

        var handler = serviceProvider.GetService(handlerType);
        if (handler is null)
        {
            throw new InvalidOperationException(
                $"Job handler type {handlerType.FullName} is not registered in the DI container: its assembly was " +
                $"loaded after the service provider was built. {AssemblyDiscoveryHint}");
        }

        if (handler is not IJobMasterHandler jobHandler)
        {
            throw new InvalidOperationException($"Job handler type {handlerType} does not implement IJobMasterHandler");
        }

        return jobHandler;
    }

    /// <summary>
    /// Returns the definition ID for <paramref name="type"/>: the value from a
    /// <see cref="JobDefinitionConfigAttribute"/> if present, otherwise from
    /// <see cref="JobMasterDefinitionIdAttribute"/> if present, otherwise <see cref="Type.FullName"/>.
    /// </summary>
    public static string GetJobDefinitionId(this Type type)
    {
        return JobDefinitionConfigAttribute.TryGetAppliedConfig(type)?.JobDefinitionId
               ?? type.GetCachedAttribute<JobMasterDefinitionIdAttribute>()?.JobDefinitionId
               ?? type.FullName!;
    }
}
