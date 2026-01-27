using System.ComponentModel;
using System.Collections.Concurrent;

namespace JobMaster.Sdk.Abstractions.Models.RecurringSchedules;

internal static class StaticRecurringDefinitionIdsKeeper
{
    private static readonly ConcurrentDictionary<string, ConcurrentDictionary<string, byte>> Data = new();

    public static void Add(string clusterId, string id)
    {
        Data.GetOrAdd(clusterId, _ => new ConcurrentDictionary<string, byte>())[id] = 1;
    }

    public static void Remove(string clusterId, string id)
    {
        if (!Data.TryGetValue(clusterId, out var map)) return;
        map.TryRemove(id, out _);
    }

    public static IReadOnlyCollection<string> GetAll(string clusterId)
    {
        if (!Data.TryGetValue(clusterId, out var map)) return Array.Empty<string>();
        return map.Keys.ToArray();
    }

    public static bool IsEmpty(string clusterId)
    {
        return !Data.TryGetValue(clusterId, out var map) || map.IsEmpty;
    }

    public static int Count(string clusterId)
    {
        return Data.TryGetValue(clusterId, out var map) ? map.Count : 0;
    }

    public static void ClearCluster(string clusterId)
    {
        Data.TryRemove(clusterId, out _);
    }
}
