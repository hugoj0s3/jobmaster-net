using JobMaster.Abstractions;
using JobMaster.Abstractions.Serialization;

namespace JobMaster.Sdk.Abstractions.Ioc.Definitions;

internal static class BootstrapBlueprintDefinitions
{
    public static readonly IList<ClusterDefinition> Clusters = new List<ClusterDefinition>();
    public static IJobMasterScheduler? JobMasterScheduler { get; set; }
    public static IJobMasterMessageJsonSerializer? JobMasterJsonSerializer { get; set; }
    
    public static IJobMasterRuntime? JobMasterRuntime { get; set; }

    public static void Clear()
    {
        Clusters.Clear();
    }
}