using System.ComponentModel;
using JobMaster.Sdk.Abstractions.Ioc;

namespace JobMaster.Sdk.Abstractions;

[EditorBrowsable(EditorBrowsableState.Never)]
public static class JobMasterClusterAwareComponentFactories
{
    private static readonly object StaticLock = new();
    private static IDictionary<string, IJobMasterClusterAwareComponentFactory> factories = new Dictionary<string, IJobMasterClusterAwareComponentFactory>();
    
    public static IJobMasterClusterAwareComponentFactory GetFactory(string clusterId)
    {
        if (!factories.TryGetValue(clusterId, out var factory))
        {
            throw new KeyNotFoundException($"Cluster component factory not found for cluster ID: {clusterId}");
        }
        return factory;
    }
    
    public static void AddFactory(string clusterId, IJobMasterClusterAwareComponentFactory factory)
    {
        lock (StaticLock)
        {
            factories[clusterId] = factory;
        }
        
    }
}