namespace JobMaster.Sdk.Contracts.Keys;

public static class JobMasterGlobalKey
{
    public const string JobMasterStaticId = "b2e8f5a3-1c7d-4f9b-8e2a-6d4c9b1f7e3a";
    public const string JobMasterNamespace = "JobMaster";
    public static readonly JobMasterNamespaceUniqueKey
        Key = 
        new(JobMasterNamespace, JobMasterStaticId);
}