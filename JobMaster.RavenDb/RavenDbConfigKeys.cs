using JobMaster.Sdk.Abstractions.Keys;

namespace JobMaster.RavenDb;

internal static class RavenDbConfigKeys
{
    public static readonly JobMasterNamespaceUniqueKey NamespaceUniqueKey =
        new("JobMaster.RavenDb", "4f6b6e8c-2b41-4c7a-9e3d-6a2f1b9c8d47");

    public static string CollectionPrefixKey => "CollectionPrefix";

    public const string DefaultCollectionPrefix = "JM_";
}
