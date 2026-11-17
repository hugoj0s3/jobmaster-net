namespace JobMaster.Sdk.Abstractions.Ioc.Definitions;

internal static class BootstrapBlueprintDefinitions
{
    public static readonly IList<ClusterDefinition> Clusters = new List<ClusterDefinition>();

    public static void Clear()
    {
        Clusters.Clear();
    }
}