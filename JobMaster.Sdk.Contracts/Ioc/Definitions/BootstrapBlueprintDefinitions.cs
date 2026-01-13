using JobMaster.Contracts;
using JobMaster.Contracts.Serialization;

namespace JobMaster.Sdk.Contracts.Ioc.Definitions;

public static class BootstrapBlueprintDefinitions
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