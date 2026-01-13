using System.Reflection;
using System.Collections.Concurrent;
using JobMaster.Contracts.Extensions;

namespace JobMaster.Contracts.Models.Attributes;

[AttributeUsage(AttributeTargets.Class)]
public class JobMasterDefinitionIdAttribute : Attribute
{
    public JobMasterDefinitionIdAttribute(string jobDefinitionId)
    {
        JobDefinitionId = jobDefinitionId;
    }

    public string JobDefinitionId { get; }
    
    private static readonly ConcurrentDictionary<string, Type> JobDefinitionIdMap = new();
    public static Type? GetJobHandlerTypeFromId(string jobdefinitionId)
    {
        if (JobDefinitionIdMap.TryGetValue(jobdefinitionId, out var result))
        {
            return result;
        }

        var type = typeof(IJobHandler);
        var types = AppDomain.CurrentDomain.GetAssemblies()
            .SelectMany(s => s.GetTypes())
            .Where(p => type.IsAssignableFrom(p))
            .Where(p => p.IsAbstract == false && p is { IsInterface: false, IsClass: true })
            .ToList();

        result = types.FirstOrDefault(x =>
                     x.GetCustomAttributes<JobMasterDefinitionIdAttribute>().FirstOrDefault()?.JobDefinitionId == jobdefinitionId) ??
                 types.FirstOrDefault(x => x.FullName == jobdefinitionId);

        if (result != null)
        {
            JobDefinitionIdMap.TryAdd(jobdefinitionId, result);
        }

        return result;
    }
    
    public static string GetJobDefinitionId(Type type)
    {
        return type.GetCustomAttribute<JobMasterDefinitionIdAttribute>()?.JobDefinitionId ?? type.FullName!;
    }
}